# -*- coding: utf-8 -*-

import json
import time
import traceback
import boto3


queue_name = "scheduler_job_queue"
table_name = "JobScheuler"


def handler(event, context):
    # Your code goes here!
    print event

    for record in event["Records"]:
        if record["eventSource"] == "aws:dynamodb":
            handle_dynamodb(record, context)
        elif record["eventSource"] == "aws:s3":
            handle_s3(record, context)


def handle_s3(record, context):
    global queue_name
    global table_name
    sqs = boto3.client("sqs")
    queue_url = None
    try:
        response = sqs.get_queue_url(
            QueueName=queue_name,
        )
        queue_url = response["QueueUrl"]
    except:
        print traceback.format_exc()
    print "S3 record: ", record
    if record["eventName"].startswith("ObjectCreated"):
        jobid       = str(time.time() * 1000000)
        jobname     = "s3:objectcreate:"
        jobdesc     = "create a boject on s3"
        jobexecutor = {"module_name"  : {"S": "s3"},
                       "class_name"   : {"S": "none"},
                       "handler_name" : {"S": "objectcreate"},}
        jobparam    = {"bucket_name"  : {"S": record["s3"]["bucket"]["name"]},
                       "bucket_arn"   : {"S": record["s3"]["bucket"]["arn"]},
                       "object_key"   : {"S": record["s3"]["object"]["key"]},
                       "object_size"  : {"N": str(record["s3"]["object"]["size"])},}
        jobqueue    = queue_url
        jobstage    = "create"

        client      = boto3.client("dynamodb")
        result      = client.put_item(
            TableName=table_name,
            Item={
                "Jobid"       : {"S" : jobid},
                "Jobname"     : {"S" : jobname},
                "Jobdesc"     : {"S" : jobdesc},
                "Jobexecutor" : {"M" : jobexecutor},
                "Jobparam"    : {"M" : jobparam},
                "Jobqueue"    : {"S" : jobqueue},
                "Jobstage"    : {"S" : jobstage},
            }
        )


def handle_dynamodb(record, context):
    sqs = boto3.client("sqs")
    print "Stream record: ", record
    try:
        if record['eventName'] == "INSERT":
            jobid = record["dynamodb"]["NewImage"]["Jobid"]["S"]
            jobname = record["dynamodb"]["NewImage"]["Jobname"]["S"]
            jobdesc = record["dynamodb"]["NewImage"]["Jobdesc"]["S"]
            jobexec = record["dynamodb"]["NewImage"]["Jobexecutor"]["M"]
            jobparam = record["dynamodb"]["NewImage"]["Jobparam"]["M"]
            jobqueue = record["dynamodb"]["NewImage"]["Jobqueue"]["S"]
            jobstage = record["dynamodb"]["NewImage"]["Jobstage"]["S"]

            if jobstage == "finish":
                return

            message = {
                "Jobid"    : jobid,
                "Jobname"  : jobname,
                "Jobdesc"  : jobdesc,
                "Jobexec"  : jobexec,
                "Jobparam" : jobparam,
                "Jobstage" : jobstage,
            }

            if jobqueue.startswith("http"):
                sqs.send_message(
                    QueueUrl=jobqueue,
                    MessageBody=json.dumps(message)
                )
            else:
                pass
    except:
        print traceback.format_exc()
