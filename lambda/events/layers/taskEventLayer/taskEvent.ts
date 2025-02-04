export interface SnsEvelope {
    requestId: string,
    requestLambdaId: string,
    origin: string,
    date: number,
    content: string
}

export enum EventTypeEnum {
    BATCH_TASK = "BATCH_TASK",
    SINGLE_TASK = "SINGLE_TASK"
}

export enum ActionTypeEnum {
    INSERT = "INSERT",
    UPDATE = "UPDATE",
    DELETE = "DELETE"
}

export interface TodoTaskEventDto {
    eventType: EventTypeEnum,
    actionType: ActionTypeEnum,
    taskId: string,
    title: string,
    owner: {
        ownerName: string,
        email: string
    },
    createdBy: {
        creatorName: string,
        email: string
    }
}