# -*- encoding: utf-8 -*-

import time
import traceback

import boto3


###
### https://medium.com/@datitran/quickstart-pyspark-with-anaconda-on-aws-660252b88c9a
### http://www.dedunu.info/2016/03/how-to-create-emr-cluster-using-boto3.html
###

default_region  = "us-west-2" # US West (Oregon)
# default_region  = "ap-northeast-1" # Asia Pacific (Tokyo)

EMR_CONFIG = {"Name"              : "boto3_emr_spark",
              "LogUri"            : "",
              "ReleaseLabel"      : "emr-5.7.0",
              "Instances"         : {"MasterInstanceType"          : "m3.xlarge",
                                     "SlaveInstanceType"           : "m3.xlarge",
                                     "InstanceCount"               : 3,
                                     "KeepJobFlowAliveWhenNoSteps" : True,
                                     "TerminationProtected"        : False,
                                     "Ec2KeyName"                  : "example",
                                     "Ec2SubnetId"                 : "subnet-1e125857",
                                    },
              "Applications"      : [{"Name" : "Spark"}],
              "VisibleToAllUsers" : True,
              "JobFlowRole"       : "EMR_EC2_DefaultRole",
              "ServiceRole"       : "EMR_DefaultRole",
             }


def create_emr():
    """create spark cluster"""
    global EMR_CONFIG
    client = boto3.client("emr", region_name=default_region)
    client.run_job_flow(
        
    )

def main():
    sts = boto3.client("sts", region_name=default_region)
    accountid = sts.get_caller_identity()["Account"]

    create_kinesis()
    create_emr()

if __name__ == "__main__":
    main()
