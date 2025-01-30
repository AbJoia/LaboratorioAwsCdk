import {DocumentClient} from 'aws-sdk/clients/dynamodb';

export enum TaskStatusEnum {
    PENDING = 'PENDING',
    COMPLETED = 'COMPLETED',
    ABANDONED = 'ABANDONED'
}

export interface TodoTaskModelDb {
    pk: string
    sk: string
    email: string
    createdAt: number
    title: string
    description: string
    archived?: boolean
    taskStatus: TaskStatusEnum
    owner: {
        ownerName: string
        email: string
    },
    assignedBy: {
        assignedByName: string
        email: string
    }
}

export class TodoTaskRepository {
    private ddbClient: DocumentClient
    private tasksDdb: string

    constructor(ddbClient: DocumentClient, tasksDdb: string){
        this.ddbClient = ddbClient
        this.tasksDdb = tasksDdb
    }

    async getAllTasks(){
        const data = await this.ddbClient.scan({
            TableName: this.tasksDdb
        }).promise()

        return data.Items as TodoTaskModelDb[]
    }

    async getTaskByEmail(email: string){
        const data = await this.ddbClient.scan({
            TableName: this.tasksDdb,
            FilterExpression: "email = :email",
            ExpressionAttributeValues: {
                ":email": email
            }
        }).promise()

        return data.Items as TodoTaskModelDb[]
    }

    async getTaskByPkAndEmail(email: string, pk:string){
        const data = await this.ddbClient.get({
            TableName: this.tasksDdb,
            Key: {
                pk: pk,
                sk: email
            }
        }).promise()

        if(data.Item) return data.Item as TodoTaskModelDb
        throw new Error("Task not found")
    }

    async creatTask(taskModel: TodoTaskModelDb): Promise<TodoTaskModelDb>{
        await this.ddbClient.put({
            TableName: this.tasksDdb,
            Item: taskModel
        }).promise()

        return taskModel
    }

    async updateTask(email: string, pk: string, taskStatus: TaskStatusEnum){
        const data = await this.ddbClient.update({
            TableName: this.tasksDdb,
            Key: {
                pk: pk,
                sk: email
            },
            ConditionExpression: "attribute_exists(pk)",
            UpdateExpression: "SET taskStatus = :taskStatus, archived = :archived",
            ExpressionAttributeValues: {
                ":taskStatus": taskStatus,
                ":archived": true
            },
            ReturnValues: "ALL_NEW"
        }).promise()

        if(data.Attributes) return data.Attributes as TodoTaskModelDb
        throw new Error("Task not found")
    }

    async deleteTask(email: string, pk: string){
        const data = await this.ddbClient.delete({
            TableName: this.tasksDdb,
            Key: {
                pk: pk,
                sk: email
            },
            ReturnValues: "ALL_OLD"
        }).promise()

        if(data.Attributes) return data.Attributes as TodoTaskModelDb
        throw new Error("Task not found")
    }
}