"""
Functions used to operate sqs
"""

import time
import logging
import traceback

def receive_from_simple(client, queue_url):
    """receive message from a simple queue
    """
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=300,
        WaitTimeSeconds=10,
    )
    return response


def receive_from_fifo(client, queue_url, receive_attempt_id):
    """receive message from a fifo queue
    """
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


def receive_message(client, workerid, queue_url):
    """recieve message from sqs
    """
    if queue_url.endswith(".fifo"):
        receive_attempt_id = "%s_%s", (workerid, str(time.time()),)
        response = receive_from_fifo(client, queue_url, receive_attempt_id)
    else:
        response = receive_from_simple(client, queue_url)
    return response


def get_queue_url(client, queue_name, account_id):
    """get queue url
    """
    try:
        response = client.get_queue_url(
            QueueName=queue_name,
            QueueOwnerAWSAccountId=account_id
        )
        return response["QueueUrl"]
    except:
        logging.warn(traceback.format_exc())
        return None


def delete_message(client, queue_url, receipt_handle):
    """delete message
    """
    response = client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    return response


def change_message_visibility(client, queue_url, receipt_handle, timeout):
    """change message visibility
    """
    response = client.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=timeout
    )
    return response