# -*- encoding: utf-8 -*-


import os
import traceback
import zipfile
from string import Template

import boto3
import json

# default_region  = "us-west-2" # US West (Oregon)
default_region  = "ap-northeast-1" # Asia Pacific (Tokyo)

lambda_file      = "lambda/lambda.py"
zip_file         = "lambda/lambda.zip"

role_name        = "etl4aws_lambda_role"
policy_name      = "etl4aws_lambda_policy"
function_name    = "etl4aws_lambda"
function_alias   = "etl4aws_lambda_alias"
runtime          = "python2.7"
handler          = "lambda.handler"

table_name       = "JobScheuler"
region           = "us-west-2"

bucket_name      = "boto3s3example"

api              = {"name"       : "jobapi",
                    "path"       : "/jobs",
                    "method"     : "POST",
                    "integrationMethod" : "POST",
                    "desc"       : "save job into dynamodb",
                    "type"       : "AWS_PROXY",
                    "uri"        : Template(
                        "arn:aws:apigateway:${region}:lambda:path/2015-03-31/functions/arn:aws:lambda:${region}:${accountid}:function:${function_name}/invocations"),
                    "lambda_sid" : "create_apigateway_mapping",
                   }

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
        },
        {
            "Effect": "Allow",
            "Action": "execute-api:*",
            "Resource": "arn:aws:execute-api:${region}:${accountid}:*/*/*/*"
        }
    ]
}""")


def create_lambda_role():
    global role_name
    client = boto3.client("iam", region_name=default_region)
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
             "Service": ["lambda.amazonaws.com","apigateway.amazonaws.com"]
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

    client = boto3.client("iam", region_name=default_region)
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
    client = boto3.client("iam", region_name=default_region)
    roleobj = client.get_role(RoleName=role_name)

    # 添加 lambda
    client = boto3.client("lambda", region_name=default_region)

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

    client = boto3.client("dynamodb", region_name=default_region)
    tableobj = None
    try:
        tableobj = client.describe_table(TableName=table_name)
    except:
        print traceback.format_exc()


    client = boto3.client("lambda", region_name=default_region)
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

    client = boto3.client("lambda", region_name=default_region)
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

    client = boto3.client("s3", region_name=default_region)
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


def create_apigateway(accountid):
    global api
    global table_name
    global function_name
    global region

    # 查找 role
    client = boto3.client("iam", region_name=default_region)
    roleobj = client.get_role(RoleName=role_name)

    client = boto3.client("apigateway", region_name=default_region)

    response = client.get_rest_apis()
    api_id = ""
    if "items" in response:
        for tmp in response["items"]:
            if tmp["name"] == api["name"]:
                api_id = tmp["id"]

    if api_id == "":
        response = client.create_rest_api(name=api["name"],
                                          description=api["desc"],
                                          version='0.1.0',)
        api_id = response["id"]

    resource_dirname = os.path.dirname(api["path"])
    resource_basename = os.path.basename(api["path"])

    response = client.get_resources(restApiId=api_id)
    print response
    resource_id = ""
    resource_methods = {}
    parent_id = ""
    if "items" in response:
        for tmp in response["items"]:
            if tmp["path"] == api["path"]:
                resource_id = tmp["id"]
                if "resourceMethods" in tmp:
                    resource_methods = tmp["resourceMethods"]
            elif tmp["path"] == resource_dirname:
                parent_id = tmp["id"]

    if resource_id == "" and parent_id != "":
        response = client.create_resource(restApiId=api_id,
                                          parentId=parent_id,
                                          pathPart=resource_basename,)
        resource_id = response["id"]

    if api["method"] not in resource_methods:
        response = client.put_method(restApiId=api_id,
                                     resourceId=resource_id,
                                     httpMethod=api["method"],
                                     authorizationType='NONE',
                                     apiKeyRequired=False,
                                     operationName='string',)
        resopnse = client.put_method_response(restApiId=api_id,
                                              resourceId=resource_id,
                                              httpMethod=api["method"],
                                              statusCode="200",
                                              responseModels={"application/json": "Empty"})

    api_uri = api["uri"].substitute(accountid=accountid,
                                    table_name=table_name,
                                    function_name=function_name,
                                    region=region,)

    response = client.put_integration(restApiId=api_id,
                                      resourceId=resource_id,
                                      httpMethod=api["method"],
                                      type=api["type"],
                                      integrationHttpMethod=api["integrationMethod"],
                                      credentials=roleobj["Role"]["Arn"],
                                      uri=api_uri,)
    response = client.put_integration_response(restApiId=api_id,
                                               resourceId=resource_id,
                                               httpMethod=api["method"],
                                               statusCode="200",
                                               responseTemplates={"application/json":""},)


    print "----", client.get_integration(restApiId=api_id,
                                         resourceId=resource_id,
                                         httpMethod="POST",)

    print response


def create_apigateway_mapping(accountid):
    global function_name
    global api
    global region

    client = boto3.client("apigateway", region_name=default_region)

    response = client.get_rest_apis()
    api_id = ""
    if "items" in response:
        for tmp in response["items"]:
            if tmp["name"] == api["name"]:
                api_id = tmp["id"]

    client = boto3.client("lambda", region_name=default_region)
    lambdaobj = client.get_function(FunctionName=function_name)

    source_arn_template = Template(
        "arn:aws:execute-api:${region}:${accountid}:${apiid}/*/${method}/${resourcepath}")
    if api["path"] == "/":
        resourcepath = ""
    else:
        resourcepath = api["path"][1:]
    source_arn = source_arn_template.substitute(region=region,
                                                accountid=accountid,
                                                apiid=api_id,
                                                method=api["method"],
                                                resourcepath=resourcepath)

    policies = client.get_policy(FunctionName=function_name)
    policies = json.loads(policies["Policy"])

    for tmp in policies["Statement"]:
        if tmp["Sid"] == api["lambda_sid"]:
            client.remove_permission(FunctionName=function_name,
                                     StatementId=api["lambda_sid"],)
            break

    permission = client.add_permission(FunctionName=function_name,
                                       Action="lambda:InvokeFunction",
                                       Principal="apigateway.amazonaws.com",
                                       SourceArn=source_arn,
                                       SourceAccount=accountid,
                                       StatementId=api["lambda_sid"],)
    print permission


def main():
    sts = boto3.client("sts", region_name=default_region)
    accountid = sts.get_caller_identity()["Account"]

    roleobj = create_lambda_role()
    add_policy(accountid)

    preparehandler()

    deploy_lambda()
    
    create_lambda_mapping()
    create_s3_bucket_mapping(accountid)
    cleanhandler()
    create_apigateway(accountid)
    create_apigateway_mapping(accountid)

if __name__ == "__main__":
    main()
