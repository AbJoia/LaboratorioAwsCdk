import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from "aws-lambda";
import { DocumentClient } from "aws-sdk/clients/dynamodb";
import { TodoTaskRepository, TodoTaskModelDb, TaskStatusEnum } from "../../lambda/tasks/layers/todoTaskLayer/todoTaskLayerRepository"
import { TodoTaskPostRequest, TodoTaskPutRequest } from "./layers/todoTaskDtoLayer/todoTaskDtoLayer";

const taskDdbTableName = process.env.TASK_DDB!
const ddbClient = new DocumentClient()
const taskRepository = new TodoTaskRepository(ddbClient, taskDdbTableName)

export async function handler(event: APIGatewayProxyEvent, context: Context)
    : Promise<APIGatewayProxyResult> {

    const apiRequestId = event.requestContext.requestId
    const lambdaId = context.awsRequestId
    const httpMethod = event.httpMethod

    console.log(`API RequestId: ${apiRequestId} - LambdaId: ${lambdaId}`)
    console.log(JSON.stringify(event));

    if (httpMethod === "GET") {
        const emailParameter = event.queryStringParameters?.email
        const taskIdParameter = event.queryStringParameters?.taskid

        if (emailParameter) {
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
            const result = await taskRepository.creatTask(taskModel)

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

        if (httpMethod === "PUT") {
            const statusRequest = JSON.parse(event.body!) as TodoTaskPutRequest

            try {

                const newStatus = statusRequest.newStatus as TaskStatusEnum
                const result = await taskRepository.updateTask(emailPathParameter, idPathParameter, newStatus)

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