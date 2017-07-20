# -*- encoding: utf-8 -*-


import os
import traceback
import zipfile
from string import Template

import boto3

lambda_file      = "lambda/lambda.py"
zip_file         = "lambda/lambda.zip"

role_name        = "etl4aws_lambda_role"
policy_name      = "etl4aws_lambda_policy"
function_name    = "etl4aws_lambda"
function_alias   = "etl4aws_lambda_alias"
runtime          = "python2.7"
handler          = "lambda.handler"

table_name="JobScheuler"
region="us-west-2"

bucket_name = "boto3s3example"

policy_template = Template("""{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "arn:aws:lambda:${region}:${accountid}:function:${function_name}*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:${region}:${accountid}:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams"
            ],
            "Resource": "arn:aws:dynamodb:${region}:${accountid}:table/${table_name}/stream/*"
        },
        {
            "Effect":   "Allow",
            "Action":   "dynamodb:*",
            "Resource": "arn:aws:dynamodb:${region}:${accountid}:table/${table_name}"
        },
        {
            "Effect":   "Allow",
            "Action":   "s3:*",
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect":   "Allow",
            "Action":   "sqs:*",
            "Resource": "*"
        }
    ]
}""")


def create_lambda_role():
    global role_name
    client = boto3.client("iam")
    # 查询 role 是否存在
    roleobj = None
    try:
        roleobj = client.get_role(RoleName=role_name)
    except:
        traceback.format_exc()

    if roleobj is None:
        # 创建 role
        newrole = client.create_role(
            RoleName=role_name,
            Path="/service-role/",
            AssumeRolePolicyDocument="""{
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "lambda.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         }
       ]
     }""",
            Description="etl4aws"
        )
        roleobj = newrole
    return roleobj


def add_policy(accountid):
    global role_name
    global policy_name
    global table_name
    global function_name
    global region

    client = boto3.client("iam")
    # 添加 policy
    policy_template.substitute(
        accountid=accountid,
        table_name=table_name,
        function_name=function_name,
        region=region,
    )

    newpolicy = client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=policy_template.substitute(
            accountid=accountid,
            table_name=table_name,
            function_name=function_name,
            region=region,
        ),
    )
    return newpolicy

def preparehandler():
    global lambda_file
    global zip_file

    with zipfile.ZipFile(zip_file, "w") as myzip:
        myzip.write(lambda_file, os.path.basename(lambda_file))

def cleanhandler():
    global zip_file

    os.remove(zip_file)

def deploy_lambda():
    global zip_file
    global function_name
    global role_name

    byte_stream = None
    with open(zip_file) as f_obj:
        byte_stream = f_obj.read()

    if byte_stream is None:
        return

    # 查找 role
    client = boto3.client("iam")
    roleobj = client.get_role(RoleName=role_name)

    # 添加 lambda
    client = boto3.client("lambda")

    # 查询 lambda 是否存在
    lambdaobj = None
    try:
        lambdaobj = client.get_function(
            FunctionName=function_name
        )
    except:
        print traceback.format_exc()

    if lambdaobj is None:
        newlambda = client.create_function(
            FunctionName=function_name,
            Runtime=runtime,
            Role=roleobj["Role"]["Arn"],
            Handler=handler,
            Code={
                'ZipFile': byte_stream,
            },
            Timeout=5,
            Publish=False,
        )
        lambdaobj = newlambda
    return lambdaobj

def create_lambda_mapping():
    global function_name
    global table_name

    client = boto3.client("dynamodb")
    tableobj = None
    try:
        tableobj = client.describe_table(TableName=table_name)
    except:
        print traceback.format_exc()


    client = boto3.client("lambda")
    # 检查映射是否创建
    mappings = client.list_event_source_mappings(
        EventSourceArn=tableobj["Table"]["LatestStreamArn"],
        FunctionName=function_name
    )
    print mappings
    print tableobj["Table"]["LatestStreamArn"]
    if len(mappings["EventSourceMappings"]) == 0:
        # 设置 event source 到 lambda function
        mapping = client.create_event_source_mapping(
            EventSourceArn=tableobj["Table"]["LatestStreamArn"],
            FunctionName=function_name,
            Enabled=True,
            BatchSize=1,
            StartingPosition="TRIM_HORIZON",
        )
        print mapping


def create_s3_bucket_mapping(accountid):
    global function_name
    global bucket_name

    client = boto3.client("lambda")
    lambdaobj = client.get_function(FunctionName=function_name)

    try:
        #增加 try 是为防止重复添加导致的异常
        permission = client.add_permission(
            FunctionName=function_name,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn="arn:aws:s3:::"+bucket_name,
            SourceAccount=accountid,
            StatementId="create_s3_bucket_mapping",
        )
        print permission
    except:
        print traceback.format_exc()

    client = boto3.client("s3")
    mapping = client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn" : lambdaobj["Configuration"]["FunctionArn"],
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
        },
    )
    print mapping


def main():
    sts = boto3.client("sts")
    accountid = sts.get_caller_identity()["Account"]

    roleobj = create_lambda_role()
    add_policy(accountid)

    preparehandler()

    deploy_lambda()
    
    create_lambda_mapping()
    create_s3_bucket_mapping(accountid)
    cleanhandler()

if __name__ == "__main__":
    main()
