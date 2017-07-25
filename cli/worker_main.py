# -*- encoding: utf-8 -*-

import traceback
import time
import signal
import os
import sys
import json
import threading
import logging
import logging.config
import socket
import ConfigParser

import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from common import toolbar_lib
from aws_operator import dynamodb
from aws_operator import sqs
from aws_operator import sts

logging.config.fileConfig('etc/worker_logging.conf')

class CodeChangeHandler(FileSystemEventHandler):
    """code change handler"""
    def __init__(self):
        super(CodeChangeHandler, self).__init__()
        self.codechanged = {}
        self.rlock = threading.RLock()

    def on_created(self, event):
        with self.rlock:
            if event.src_path.endswith(".py"):
                self.codechanged[event.src_path] = event
                logging.info("code create: %s", event.src_path)

    def on_modified(self, event):
        with self.rlock:
            if event.src_path.endswith(".py"):
                self.codechanged[event.src_path] = event
                logging.info("code change: %s", event.src_path)


class Worker(object):
    """class Worker
    """
    def __init__(self, config_file):
        self.config_file = config_file
        self.module_path = ""
        self.dynamo_jobschedule = ""
        self.dynamo_jobworker = ""
        self.sqs_name = ""
        self.quit_flag = False
        self.pid = os.getpid()
        self.hostname = socket.gethostname()
        self.worker_id = "worker%02d@%s" % (self.pid, self.hostname)
        self.worker_status = "idle" # idle|busy
        self.worker_job = "null"    # null|{job_id}
        self.group_status = ""
        self.worker_heartbeat_interval = 60
        self.sqs_visibility_interval = 60
        self.sqs_visibility_timeout = 90
        self.config_read_interval = 60
        self.aws_account_id = ""
        self.queue_url = ""
        self.last_read_config = 0
        self.receipt_handle = ""
        self.observer = None
        self.code_update_handler = None
        self.default_region = "us-west-2"

        self.dynamo_client = None boto3.client("dynamodb")
        self.sqs_client = None boto3.client("sqs")

        signal.signal(signal.SIGINT, self.signal_handler)

        self.worker_monitor_event = threading.Event()
        self.worker_monitor_threadobj = threading.Thread(target=self.worker_monitor)
        self.worker_monitor_threadobj.daemon = True
        self.worker_monitor_threadobj.start()

        self.sqs_monitor_event = threading.Event()
        self.sqs_monitor_threadobj = threading.Thread(target=self.sqs_monitor)
        self.sqs_monitor_threadobj.daemon = True
        self.sqs_monitor_threadobj.start()

        self.code_update_handler = CodeChangeHandler()
        self.observer = Observer()
        self.observer.start()

        self.main_stream_eveent = threading.Event()


    def signal_handler(self, signum, frame):
        """handle signals
        """
        logging.info("Signal handler called with signal %s", signum)
        self.quit_flag = True


    def config_read(self):
        """reading config file
        """
        current = time.time()
        if current - self.last_read_config < self.config_read_interval:
            return
        else:
            self.last_read_config = current

        config = ConfigParser.ConfigParser()
        config.read(self.config_file)
        self.dynamo_jobschedule = config.get("worker", "dynamo_jobschedule")
        self.dynamo_jobworker = config.get("worker", "dynamo_jobworker")
        self.sqs_name = config.get("worker", "sqs_name")
        self.worker_heartbeat_interval = config.getint("worker", "heartbeat_interval")
        self.sqs_visibility_interval = config.getint("worker", "sqs_visibility_interval")
        self.config_read_interval = config.getint("worker", "config_read_interval")
        self.default_region = config.get("worker", "default_region")

        self.dynamo_client = boto3.client("dynamodb", region_name=self.default_region)
        self.sqs_client = boto3.client("sqs", region_name=self.default_region)
        self.sts_client = boto3.client("sts", region_name=self.default_region)

        self.sqs_visibility_timeout = self.sqs_visibility_interval + 30

        self.aws_account_id = sts.get_account_id(self.sts_client)
        self.queue_url = sqs.get_queue_url(self.sqs_client, self.sqs_name, self.aws_account_id)

        new_module_path = config.get("worker", "module_path")
        if not new_module_path.startswith("/"):
            current_dir = os.getcwd()
            new_module_path = os.path.normpath(os.path.join(current_dir, new_module_path))

        if not os.path.exists(new_module_path):
            os.makedirs(new_module_path)

        if new_module_path not in sys.path:
            sys.path.append(new_module_path)

        # if module_path didn't change, we don't need to schedule it again
        if self.module_path != new_module_path:
            self.observer.unschedule_all()
            self.observer.schedule(self.code_update_handler, new_module_path, recursive=True)

        self.module_path = new_module_path

    def worker_monitor(self):
        """update worker status into dynamodb
        query worker groups status at the same time
        """
        while True:
            try:
                current_time = time.time().__trunc__()
                dynamodb.updateworker(self.dynamo_client, current_time, self.worker_id,
                                      self.worker_status, self.worker_job)

                self.group_status = dynamodb.checkgroupstatus(self.dynamo_client, self.hostname)

                self.worker_monitor_event.wait(self.worker_heartbeat_interval)
                self.worker_monitor_event.clear()
            except:
                logging.warn(traceback.format_exc())


    def sqs_monitor(self):
        """update sqs visibility timeout
        update visibility timeout every 90 second,
        and the visibility timeout is 120 second
        """
        while True:
            try:
                if self.receipt_handle:
                    sqs.change_message_visibility(self.sqs_client, self.queue_url,
                                                  self.receipt_handle,
                                                  self.sqs_visibility_interval + 30)

                self.sqs_monitor_event.wait(timeout=self.sqs_visibility_interval)
                self.sqs_monitor_event.clear()
            except:
                logging.warn(traceback.format_exc())


    def update_module(self):
        """auto update modules"""
        if self.code_update_handler.codechanged:
            with self.code_update_handler.rlock:
                for changed in self.code_update_handler.codechanged:
                    relpath = os.path.relpath(changed, self.module_path)
                    module_name = relpath.split("/")[0]
                    if module_name.endswith(".py"):
                        module_name = module_name[:-3]
                    logging.info("updating module %s, filepath: %s", module_name, changed)
                    try:
                        if module_name in sys.modules:
                            reload(sys.modules[module_name])
                        else:
                            __import__(module_name)
                    except:
                        logging.warn(traceback.format_exc())
                self.code_update_handler.codechanged = {}


    def execute(self, job):
        """execute jobs
        """
        logging.info("-----execute----worker:%s, job: %s", self.worker_job, job)
        try:
            jobid = job["Jobid"]
            jobname = job["Jobname"]
            jobparam = job["Jobparam"]
            jobexec = job["Jobexec"]
            jobstage = job["Jobstage"]

            failed_reason = ""

            # query dynamodb for more job details to check if the job exists
            # if exists execute, if not return True
            job = dynamodb.getjobbyid(self.dynamo_client, jobid)
            if job is None:
                logging.info("Job ID(%s) doesn't exist in dynamodb", jobid)
                return True

            # before execute the job update the stage
            # hope this can prevent other worker to run the same job
            try:
                dynamodb.updatejobstage(self.dynamo_client, jobid,
                                        ["create", "failed"], "running", "running")
            except:
                logging.warn(traceback.format_exc())
                return False

            module_name = "module_name" in jobexec and jobexec["module_name"]["S"] or None
            class_name = "class_name" in jobexec and jobexec["class_name"]["S"] or None
            handler_name = "handler_name" in jobexec and jobexec["handler_name"]["S"] or None

            if module_name is None:
                failed_reason = "job param invalid: missing param module_name"
                logging.info("Job ID(%s) failed, cause %s", jobid, failed_reason)
                dynamodb.updatejobstage(self.dynamo_client,
                                        self.worker_job, "running", "failed", failed_reason)
                return False

            if handler_name is None:
                failed_reason = "job param invalid: missing param handler_name"
                logging.info("Job ID(%s) failed, cause %s", jobid, failed_reason)
                dynamodb.updatejobstage(self.dynamo_client,
                                        self.worker_job, "running", "failed", failed_reason)
                return False

            if module_name in sys.modules:
                module_obj = sys.modules[module_name]
            else:
                module_obj = __import__(module_name)

            if class_name is None or class_name == "none":
                if hasattr(module_obj, handler_name):
                    handler = getattr(module_obj, handler_name)
                    handler(jobid, jobparam)
                    dynamodb.updatejobstage(self.dynamo_client,
                                            self.worker_job, "running", "finish", "success")
                    return True
            else:
                if hasattr(module_obj, class_name):
                    class_obj = getattr(module_obj, class_name)
                    obj_obj = class_obj()
                    if hasattr(obj_obj, handler_name):
                        handler = getattr(obj_obj, handler_name)
                        handler(jobid, jobparam)
                        dynamodb.updatejobstage(self.dynamo_client,
                                                self.worker_job, "running", "finish", "success")
                    return True
        except:
            logging.warn(traceback.format_exc())
            dynamodb.updatejobstage(self.dynamo_client,
                                    self.worker_job, "running", "failed", traceback.format_exc())
            return False


    def run(self):
        """the main process
        """
        while True:
            try:
                if self.quit_flag:
                    return

                # reading config file
                self.config_read()

                # update code
                self.update_module()

                # read sqs
                jobs = []
                if self.group_status == "enable":
                    messages = sqs.receive_message(self.sqs_client, self.worker_id, self.queue_url)
                    if "Messages" in messages:
                        for message in messages["Messages"]:
                            msg_id = message["MessageId"]
                            msg_handle = message["ReceiptHandle"]
                            job_body = json.loads(message["Body"])
                            jobs.append((msg_id, msg_handle, job_body))

                    # execute jobs
                    for (msg_id, msg_handle, job_body) in jobs:
                        # update worker status using thread
                        self.worker_job = job_body["Jobid"]
                        self.worker_status = "busy"
                        self.worker_monitor_event.set()

                        # update visibility timeout using thread
                        self.receipt_handle = msg_handle
                        self.sqs_monitor_event.set()

                        self.execute(job_body)

                        sqs.delete_message(self.sqs_client, self.queue_url, msg_handle)
                else:
                    logging.info("group %s disabled", self.hostname)

                self.worker_status = "idle"
                self.worker_job = "null"
                self.receipt_handle = ""

                time.sleep(10)
            except:
                logging.warn(traceback.format_exc())


def main():
    """main function"""
    config_file = toolbar_lib.check_para(sys.argv, "f", "etc/worker.ini")
    worker_obj = Worker(config_file)
    worker_obj.run()

    #before stop, close the observer
    if worker_obj.observer is not None:
        worker_obj.observer.stop()
        worker_obj.observer.join()


if __name__ == "__main__":
    main()
