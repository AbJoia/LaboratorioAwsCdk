import { APIGatewayEventDefaultAuthorizerContext } from "aws-lambda"
import {CognitoIdentityServiceProvider} from "aws-sdk"

export class AuthService {

    private cognitoIdentityServiceProvider: CognitoIdentityServiceProvider

    constructor(cognitoIdentityServiceProvider: CognitoIdentityServiceProvider){  
        this.cognitoIdentityServiceProvider = cognitoIdentityServiceProvider    
    }

    async getUserEmail(authorizer: APIGatewayEventDefaultAuthorizerContext){
        const userPoolId = authorizer?.claims.iss.split("amazonaws.com/")[1]
        const userName = authorizer?.claims.username

        const user = await this.cognitoIdentityServiceProvider.adminGetUser({
            Username: userName,
            UserPoolId: userPoolId
        }).promise()

        const email = user.UserAttributes?.find(attr => attr.Name === 'email')?.Value
        if(email) return email

        throw new Error('Email not found')
    }

    isAdminUser(authorizer: APIGatewayEventDefaultAuthorizerContext){
        return authorizer?.claims.scope.startsWith('admin')
    }
}