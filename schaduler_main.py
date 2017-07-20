# -*- encoding: utf-8 -*-

import boto3
import time
import traceback


table_name = "JobScheuler"
queue_name = "scheduler_job_queue"

def add_job(client, jobname, jobdesc, jobexecutor, jobparam, jobqueue):
    global table_name
    jobid = str(time.time() * 1000000)
    result = client.put_item(
        TableName=table_name,
        Item={
            "Jobid"       : {"S" : jobid},
            "Jobname"     : {"S" : jobname},
            "Jobdesc"     : {"S" : jobdesc},
            "Jobexecutor" : {"M" : jobexecutor},
            "Jobparam"    : {"M" : jobparam},
            "Jobqueue"    : {"S" : jobqueue},
            "Jobstage"    : {"S" : "create"},
        }
    )
    return result


def main():
    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]

    client = boto3.client("sqs")
    queue_url = client.get_queue_url(QueueName=queue_name,
                                     QueueOwnerAWSAccountId=account_id)

    dynamodb = boto3.client("dynamodb")
    add_job(dynamodb, "example", "example", {"module_name"  : {"S": "command"},
                                             "class_name"   : {"S": "none"},
                                             "handler_name" : {"S": "simple_run"}},
            {"cmd"      : {"S": "ls -1"}}, queue_url["QueueUrl"])


if __name__ == "__main__":
    main()

