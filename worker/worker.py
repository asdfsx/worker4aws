# -*- encoding: utf-8 -*-

import time
import json
import sys
import socket
import threading
import traceback
import boto3

def receive_message(client, queue_url):
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=300,
        WaitTimeSeconds=10,
    )
    return response


def receive_fifo_message(client, queue_url, receive_attempt_id):
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=300,
        WaitTimeSeconds=10,
        ReceiveRequestAttemptId=receive_attempt_id
    )
    return response


def delete_message(client, queue_url, receipt_handle):
    response = client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    return response


def execute(process_id, params, module_path):
    print "-----execute----", process_id, params

    jobid = params["Jobid"]
    jobname = params["Jobname"]
    jobparam = params["Jobparam"]
    jobexec = params["Jobexec"]
    jobstage = params["Jobstage"]

    if module_path not in sys.path:
        sys.path.append(module_path)

    module_name = "module_name" in jobexec and jobexec["module_name"]["S"] or None
    class_name = "class_name" in jobexec and jobexec["class_name"]["S"] or None
    handler_name = "handler_name" in jobexec and jobexec["handler_name"]["S"] or None

    if module_name is None:
        return

    if handler_name is None:
        return

    if module_name in sys.modules:
        module_obj = sys.modules[module_name]
    else:
        module_obj = __import__(module_name)

    if class_name is None or class_name == "none":
        if hasattr(module_obj, handler_name):
            handler = getattr(module_obj, handler_name)
            handler(jobid, jobparam)
    else:
        if hasattr(module_obj, class_name):
            class_obj = getattr(module_obj, class_name)
            obj_obj = class_obj()
            if hasattr(obj_obj, handler_name):
                handler = getattr(obj_obj, handler_name)
                handler(jobid, jobparam)


def updateworker(worker_id, worker_status, worker_job):
    current_time = time.time().__trunc__()
    client = boto3.client("dynamodb")
    client.update_item(
        ExpressionAttributeNames={
            "#ws": "Workerstatus",
            "#wj": "Workerjob",
            "#ut": "Updatetime",
            "#et": "ExpirationTime",},
        ExpressionAttributeValues={
            ":ws": {"S" : worker_status},
            ":wj": {"S" : worker_job},
            ":ut": {"N" : str(current_time)},
            ":et": {"N" : str(current_time + 600)},},
        Key={"Workerid": {"S": worker_id},},
        ReturnValues='NONE',
        TableName="JobWorker",
        UpdateExpression='SET #ws = :ws, #wj = :wj, #ut = :ut, #et = :et',
    )


def workermonitor(worker_id):
    client = boto3.client("dynamodb")

    while True:
        try:
            current_time = time.time().__trunc__()
            client.update_item(
                ExpressionAttributeNames={"#et": "ExpirationTime",},
                ExpressionAttributeValues={":et": {"N" : str(current_time + 600)},},
                Key={"Workerid": {"S": worker_id},},
                ReturnValues='NONE',
                TableName="JobWorker",
                UpdateExpression='SET #et = :et',
            )
            time.sleep(60)
        except:
            print traceback.format_exc()


def run(process_id, module_path):
    sqs = None
    queue_urls = []
    queue_urls_updatetime = 0
    module_path = module_path

    hostname = socket.gethostname()
    worker_id = "worker%02d@%s" % (process_id, hostname)
    updateworker(worker_id, "alive", "none")

    threadobj = threading.Thread(target=workermonitor, args=(worker_id,))
    threadobj.daemon = True
    threadobj.start()

    while True:
        print process_id

        if sqs is None:
            sqs = boto3.client("sqs")

        if not queue_urls:
            result = sqs.list_queues()
            print "queues=======", result
            if "QueueUrls" in result:
                for url in result["QueueUrls"]:
                    queue_urls.append(url)
            queue_urls_updatetime = time.time()

        for url in queue_urls:
            if url.endswith(".fifo"):
                receive_attempt_id = str(time.time())
                response = receive_fifo_message(sqs, url, receive_attempt_id)
            else:
                response = receive_message(sqs, url)

            print "-----", response
            if "Messages" in response:
                for message in response["Messages"]:
                    json_message = json.loads(message["Body"])
                    updateworker(worker_id, "busy", json_message["Jobid"])
                    execute(worker_id, json_message, module_path)
                    delete_message(sqs, url, message["ReceiptHandle"])

                updateworker(worker_id, "alive", "none")
        if queue_urls_updatetime - time.time() > 60 * 5:
            queue_urls = []
        time.sleep(5)
