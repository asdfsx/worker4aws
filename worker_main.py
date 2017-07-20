# -*- encoding: utf-8 -*-

import multiprocessing
import traceback
import time
import signal
import os
import sys
import threading

import boto3

from worker import worker


workers = {}
quit = False
module_path = "worker4aws/modules"

module_path = os.path.join(sys.path[0], "modules")
print module_path

def signal_handler(signum, frame):
    global quit
    print 'Signal handler called with signal', signum
    quit = True


def create_workers(worker_count):
    global workers
    global module_path
    for i in range(worker_count):
        worker_process = multiprocessing.Process(target=worker.run, args=(i, module_path,))
        worker_process.start()
        workers[i] = worker_process


def send_fifo_message(client, queue_url, message):
    response = client.send_message(
                QueueUrl=queue_url,
                MessageBody=message,
                MessageDeduplicationId=str(time.time()),
                MessageGroupId='Test'
            )
    return response


def send_message(client, queue_url, message):
    response = client.send_message(
                QueueUrl=queue_url,
                MessageBody=message,
            )
    return response


def threadworker():
    queue_url = ""
    json_message="""{
    "module": "example",
    "handler": "run",
    "message": "hello!!!"
}"""

    json_message="""{
    "module": "base",
    "class": "Base",
    "handler": "run",
    "message": "hello!!!"
}"""
    
    sqs = boto3.client("sqs")
    queue_urls = []
    queue_urls_updatetime = 0

    i = 0
    while i < 10 :
        if not queue_urls:
            result = sqs.list_queues()
            print "queues======", result
            if "QueueUrls" in result:
                for url in result["QueueUrls"]:
                    queue_urls.append(url)
            queue_urls_updatetime = time.time()
            print queue_urls
        
        for queue_url in queue_urls:
            if queue_url.endswith(".fifo"):
                send_fifo_message(sqs, queue_url, json_message)
            else:
                send_message(sqs, queue_url, json_message)
            time.sleep(5)
        i += 1

def main():
    global quit
    global workers

    create_workers(2)
    signal.signal(signal.SIGINT, signal_handler)

    #threadobj = threading.Thread(target=threadworker)
    #threadobj.daemon = True
    #threadobj.start()

    while True:
        try:
            time.sleep(10)
            print "------" 
            if quit:
                for i in workers:
                    try:
                        workers[i].terminate()
                        workers[i] = None
                    except:
                        traceback.format_exc()
                return
            send_message(sqs, queu_url, json_message)
        except:
            traceback.format_exc()

if __name__ == "__main__":
    main()
