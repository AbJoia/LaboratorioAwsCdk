import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from "aws-lambda";
import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { SNS } from "aws-sdk";
import { TodoTaskRepository, TodoTaskModelDb, TaskStatusEnum } from "../../lambda/tasks/layers/todoTaskLayer/todoTaskLayerRepository"
import { TodoTaskPostRequest, TodoTaskPutRequest } from "./layers/todoTaskDtoLayer/todoTaskDtoLayer";
import { ActionTypeEnum, EventTypeEnum, SnsEvelope, TodoTaskEventDto } from "../events/layers/taskEventLayer/taskEvent";
import { AuthService } from "../auth/layers/authLayer/auth"
import { CognitoIdentityServiceProvider } from "aws-sdk"

const taskDdbTableName = process.env.TASK_DDB!
const snsTopicArn = process.env.SNS_TOPIC_ARN!
const ddbClient = new DocumentClient()
const taskRepository = new TodoTaskRepository(ddbClient, taskDdbTableName)
const snsClient = new SNS()
const cognitoIdentityServiceProvider = new CognitoIdentityServiceProvider()
const authService = new AuthService(cognitoIdentityServiceProvider)

export async function handler(event: APIGatewayProxyEvent, context: Context)
    : Promise<APIGatewayProxyResult> {

    const apiRequestId = event.requestContext.requestId
    const lambdaId = context.awsRequestId
    const httpMethod = event.httpMethod
    const userEmail = await authService.getUserEmail(event.requestContext.authorizer)
    const isAdmin = authService.isAdminUser(event.requestContext.authorizer)

    console.log(`API RequestId: ${apiRequestId} - LambdaId: ${lambdaId}`)
    console.log(JSON.stringify(event));

    if (httpMethod === "GET") {
        const emailParameter = event.queryStringParameters?.email
        const taskIdParameter = event.queryStringParameters?.taskid

        if (emailParameter) {

            if (emailParameter !== userEmail && !isAdmin) {
                return {
                    statusCode: 403,
                    body: JSON.stringify({
                        message: "Forbidden"
                    })
                }
            }

            if (taskIdParameter) {
                try {
                    const result = await taskRepository.getTaskByPkAndEmail(emailParameter, taskIdParameter)
                    if (result) {
                        return {
                            statusCode: 200,
                            body: JSON.stringify(result)
                        }
                    }
                } catch (err) {
                    console.error((<Error>err).message)
                    return {
                        statusCode: 404,
                        body: (<Error>err).message
                    }
                }
            }

            const result = await taskRepository.getTaskByEmail(emailParameter)
            return {
                statusCode: 200,
                body: JSON.stringify(result)
            }
        }

        if (!isAdmin) {
            return {
                statusCode: 403,
                body: JSON.stringify({
                    message: "Forbidden"
                })
            }
        }

        const result = await taskRepository.getAllTasks()
        return {
            statusCode: 200,
            body: JSON.stringify(result)
        }
    }

    if (httpMethod === "POST") {
        try {
            const taskRequest = JSON.parse(event.body!) as TodoTaskPostRequest
            const taskModel = buildTask(taskRequest)            

            if(taskModel.owner.email !== userEmail && !isAdmin){
                return {
                    statusCode: 403,
                    body: JSON.stringify({
                        message: "Forbidden"
                    })
                }
            }

            const result = await taskRepository.creatTask(taskModel)

            await publishToSns(
                ActionTypeEnum.INSERT,
                EventTypeEnum.SINGLE_TASK,
                result.assignedBy.assignedByName,
                result.assignedBy.email,
                result.pk,
                result.owner.ownerName,
                result.owner.email,
                result.title,
                apiRequestId,
                lambdaId,
                context.functionName
            )

            return {
                statusCode: 201,
                body: JSON.stringify(result)
            }

        } catch (error) {
            console.error((<Error>error).message)
            return {
                statusCode: 400,
                body: (<Error>error).message
            }
        }
    }

    if (event.resource === "/tasks/{email}/{id}") {
        const emailPathParameter = event.pathParameters!.email as string
        const idPathParameter = event.pathParameters!.id as string

        if(emailPathParameter !== userEmail && !isAdmin){
            return {
                statusCode: 403,
                body: JSON.stringify({
                    message: "Forbidden"
                })
            }
        }

        if (httpMethod === "PUT") {
            const statusRequest = JSON.parse(event.body!) as TodoTaskPutRequest

            try {

                const newStatus = statusRequest.newStatus as TaskStatusEnum
                const result = await taskRepository.updateTask(emailPathParameter, idPathParameter, newStatus)

                await publishToSns(
                    ActionTypeEnum.UPDATE,
                    EventTypeEnum.SINGLE_TASK,
                    result.assignedBy.assignedByName,
                    result.assignedBy.email,
                    result.pk,
                    result.owner.ownerName,
                    result.owner.email,
                    result.title,
                    apiRequestId,
                    lambdaId,
                    context.functionName
                )

                return {
                    statusCode: 204,
                    body: JSON.stringify({
                        message: `Update task sucessful. Task ID ${idPathParameter}`,
                        body: JSON.stringify(result)
                    })
                }

            } catch (error) {
                console.error((<Error>error).message)
                return {
                    statusCode: 400,
                    body: (<Error>error).message
                }
            }
        }

        if (httpMethod === "DELETE") {
            try {

                const result = await taskRepository.deleteTask(emailPathParameter, idPathParameter)

                await publishToSns(
                    ActionTypeEnum.DELETE,
                    EventTypeEnum.SINGLE_TASK,
                    result.assignedBy.assignedByName,
                    result.assignedBy.email,
                    result.pk,
                    result.owner.ownerName,
                    result.owner.email,
                    result.title,
                    apiRequestId,
                    lambdaId,
                    context.functionName
                )

                return {
                    statusCode: 204,
                    body: JSON.stringify(result)
                }

            } catch (error) {
                console.error((<Error>error).message)
                return {
                    statusCode: 400,
                    body: (<Error>error).message
                }
            }
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify({
            message: "Hello World"
        })
    }
}

function buildTask(task: TodoTaskPostRequest): TodoTaskModelDb {
    const timestamp = Date.now()
    const pk = generateUniqueId();

    return {
        pk: pk,
        sk: task.owner.email,
        createdAt: timestamp,
        title: task.title,
        email: task.owner.email,
        description: task.description,
        taskStatus: TaskStatusEnum.PENDING,
        archived: false,
        assignedBy: {
            assignedByName: task.assignedBy.name,
            email: task.assignedBy.email
        },
        owner: {
            ownerName: task.owner.name,
            email: task.owner.email
        }
    }
}

function generateUniqueId() {
    return `TID-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
}

async function publishToSns(
    actionType: ActionTypeEnum,
    eventType: EventTypeEnum,
    creatorName: string,
    creatorEmail: string,
    taskId: string,
    ownerName: string,
    ownerEmail: string,
    title: string,
    requestId: string,
    requestLambdaId: string,
    functionName: string
): Promise<any> {

    const todoTaskEventDto: TodoTaskEventDto = {
        actionType: actionType,
        eventType: eventType,
        createdBy: {
            creatorName: creatorName,
            email: creatorEmail
        },
        taskId: taskId,
        owner: {
            ownerName: ownerName,
            email: ownerEmail
        },
        title: title
    }

    const snsEnvelop: SnsEvelope = {
        requestId: requestId,
        requestLambdaId: requestLambdaId,
        origin: functionName,
        content: JSON.stringify(todoTaskEventDto),
        date: Date.now()
    }

    return snsClient.publish({
        TopicArn: snsTopicArn,
        Message: JSON.stringify(snsEnvelop)
    }).promise()
}