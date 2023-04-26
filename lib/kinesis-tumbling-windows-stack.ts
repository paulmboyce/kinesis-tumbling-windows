import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { join } from "path";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import {
  KinesisEventSource,
  SqsDlq,
} from "aws-cdk-lib/aws-lambda-event-sources";

export class KinesisTumblingWindowsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const ddbUnicornAggregation = new ddb.Table(this, "UnicornAggregation", {
      partitionKey: {
        name: "name",
        type: ddb.AttributeType.STRING,
      },
      sortKey: {
        name: "windowStart",
        type: ddb.AttributeType.STRING,
      },
      tableName: "UnicornAggregation",
      billingMode: ddb.BillingMode.PROVISIONED,
      readCapacity: 2,
      writeCapacity: 2,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // ** NOT recommended for production code **
    });

    const lambdaRole = new iam.Role(this, "WildRydesAggregatorExecutionRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      description: "Role to access kinesis and dynamodb",
      roleName: "WildRydesAggregatorExecutionRole",
    });

    lambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromManagedPolicyArn(
        this,
        "Apply.AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
      )
    );
    // AWSLambdaKinesisExecutionRole is not neccessary!
    //  >>> because CDK will add the actions from the AWSLambdaKinesisExecutionRole
    // due to addEventSource
    // but restricted to the Kinesis Stream here, which is tighter than the managaged role which
    // gets access to ALL Kinesis Streams

    lambdaRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [ddbUnicornAggregation.tableArn],
        actions: ["dynamodb:PutItem"],
      })
    );
    const lambdaFn = new lambda.Function(this, "Function", {
      functionName: "WildRydesAggregator",
      runtime: lambda.Runtime.NODEJS_14_X,
      handler: "index.handler",
      // deadLetterQueueEnabled: true,
      // deadLetterQueue: deadLetterQueue,
      code: lambda.Code.fromAsset(join(__dirname, "../lambda")),
      role: lambdaRole,
      environment: {
        TABLE_NAME: ddbUnicornAggregation.tableName,
        AWS_NODEJS_CONNECTION_REUSE_ENABLED: "1",
      },
      logRetention: 3,
    });

    const stream = new kinesis.Stream(this, "wildrydes-stream", {
      streamName: "wildrydes",
      streamMode: kinesis.StreamMode.PROVISIONED,
      shardCount: 1,
    });
    lambdaFn.addEventSource(
      new KinesisEventSource(stream, {
        batchSize: 100,
        startingPosition: lambda.StartingPosition.LATEST,
        retryAttempts: 2,
        bisectBatchOnError: true,
        tumblingWindow: cdk.Duration.seconds(60),
      })
    );

    new cdk.CfnOutput(this, "DynamoDB.UnicornAggregation.ARN:", {
      value: ddbUnicornAggregation.tableArn,
    });
    new cdk.CfnOutput(this, "LambdaFn.ARN", { value: lambdaFn.functionArn });
    new cdk.CfnOutput(this, "LambdaFn.Role.ARN", {
      value: lambdaFn.role!.roleArn,
    });
    new cdk.CfnOutput(this, "Kinesis.Stream.ARN", {
      value: stream.streamArn,
    });
  }
}
