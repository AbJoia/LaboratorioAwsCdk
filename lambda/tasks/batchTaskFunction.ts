import { Context, S3Event, S3EventRecord } from 'aws-lambda'
import { TaskStatusEnum, TodoTaskModelDb, TodoTaskRepository } from './layers/todoTaskLayer/todoTaskLayerRepository'
import { S3 } from 'aws-sdk'
import { SNS } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { ActionTypeEnum, EventTypeEnum, SnsEvelope, TodoTaskEventDto } from '../events/layers/taskEventLayer/taskEvent'

const taskDdb = process.env.TASK_DDB!
const snsTopicArn = process.env.SNS_TOPIC_ARN!
const s3Client = new S3()
const snsClient = new SNS()
const documentClient = new DocumentClient()
const todoTaskRepository = new TodoTaskRepository(documentClient, taskDdb)

export async function handler(event: S3Event, context: Context): Promise<void> {
    console.log(`RequestId: ${context.awsRequestId}`)
    console.log(`Event: ${JSON.stringify(event)}`)

    const promises: Promise<TodoTaskModelDb[]>[] = []

    event.Records.forEach(record => {
        promises.push(importBatchTasks(record))
    })

    const promisesResolved = await Promise.all(promises)

    const events: TodoTaskEventDto[] = []

    promisesResolved.forEach(promise => {
        promise.forEach(task => {
            events.push({
                actionType: ActionTypeEnum.INSERT,
                eventType: EventTypeEnum.BATCH_TASK,
                taskId: task.pk,
                title: task.title,
                createdBy: {
                    creatorName: task.assignedBy.assignedByName,
                    email: task.assignedBy.email
                },
                owner: task.owner
            })
        })
    })

    const consolidatedEvents: TodoTaskEventDto[] = []

    events.map(record => {
        const tasks = events.filter(e => e.owner.email === record.owner.email)
        if (!consolidatedEvents?.find(c => c.taskId.includes(tasks[0].taskId))) {
            consolidatedEvents.push({
                ...record,
                title: tasks.map(t => t.title).join(','),
                taskId: tasks.map(t => t.taskId).join(',')
            })
        }
    })

    const snsPromises: Promise<any>[] = []

    consolidatedEvents.forEach(event => {
        snsPromises.push(
            publishToSns(
                event.actionType,
                event.eventType,
                event.createdBy.creatorName,
                event.createdBy.email,
                event.taskId,
                event.owner.ownerName,
                event.owner.email,
                event.title,
                context.awsRequestId,
                context.awsRequestId,
                context.functionName
            ))
    })

    await Promise.all(snsPromises)
}

async function importBatchTasks(record: S3EventRecord): Promise<TodoTaskModelDb[]> {
    const BATCH_WRITE_LIMIT_DYNAMODB = 25

    const object = await s3Client.getObject({
        Bucket: record.s3.bucket.name,
        Key: record.s3.object.key
    }).promise()

    const objectData = object.Body?.toString('utf-8')

    if (!objectData)
        throw new Error('Object data is empty')

    const tasks: TodoTaskModelDb[] = []

    try {
        objectData.split('\n').forEach(line => {
            const cleanedLine = line.replace(/\r/g, '')
            const [
                title,
                description,
                deadLine,
                ownerName,
                ownerEmail,
                assignedByName,
                assignedByEmail
            ] = cleanedLine.split(',')

            const pk = generateUniqueId()
            const timestamp = Date.now()

            const task: TodoTaskModelDb = {
                pk: pk,
                sk: ownerEmail,
                createdAt: timestamp,
                description: description,
                title: title,
                email: ownerEmail,
                taskStatus: TaskStatusEnum.PENDING,
                archived: false,
                assignedBy: {
                    assignedByName: assignedByName,
                    email: assignedByEmail
                },
                owner: {
                    ownerName: ownerName,
                    email: ownerEmail
                }
            }

            console.log(`Import task InProgress ${JSON.stringify(task)}`)
            tasks.push(task)
        })

        if (tasks.length > BATCH_WRITE_LIMIT_DYNAMODB)
            throw new Error("Batch size is greater than 25")

        await todoTaskRepository.createBatchTask(tasks)
        console.log(`Import tasks successfully. Itens ${tasks.length}`)


    } catch (error) {
        console.error(`Error importing tasks: ${(<Error>error).message}`)
        throw error
    }

    return tasks
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