# -*- encoding: utf-8 -*-

import time
import traceback

import boto3

default_region  = "us-west-2" # US West (Oregon)
# default_region  = "ap-northeast-1" # Asia Pacific (Tokyo)

table_list      = [# job list
                   {"table_name"     : "JobScheuler",
                    "key_schema"     : [{"AttributeName"       : "Jobid", 
                                         "KeyType"             : "HASH"},],
                    "table_attr"     : [{"AttributeName"       : "Jobid", 
                                         "AttributeType"       : "S"},],
                    "table_schema"   : {
                        "Jobid"       : {"S" : str(time.time() * 1000000)},
                        "Jobname"     : {"S" : "example"},
                        "Jobdesc"     : {"S" : "example"},
                        "Jobexecutor" : {"M" : {"module_name"  : {"S": "example"},
                                                "class_name"   : {"S": "none"},
                                                "handler_name" : {"S": "run"}}},
                        "Jobparam"    : {"M" : {"message"      : {"S": "hello!!!"}}},
                        "Jobqueue"    : {"S" : "example"},
                        "Jobstage"    : {"S" : "finish"},
                        "Jobresult"   : {"S" : "success"},},
                    "provisioned_throughput" : {
                        "ReadCapacityUnits"  : 10,
                        "WriteCapacityUnits" : 10},
                    "stream_specification"   : {
                        "StreamEnabled"      : True,
                        "StreamViewType"     : "NEW_IMAGE"},
                    "ttlSpecification"       : None,},

                   # worker list
                   {"table_name"    : "JobWorker",
                    "key_schema"    : [{"AttributeName"       : "Workerid",
                                        "KeyType"             : "HASH"},],
                    "table_attr"     : [{"AttributeName"       : "Workerid",
                                         "AttributeType"       : "S"},],
                    "table_schema"  : {
                        "Workerid"      : {"S": "example@1.1.1.1"},
                        "Workerstatus"  : {"S": "dead"},
                        "Workerjob"     : {"S": "example"},
                        "Updatetime"    : {"N": str(time.time().__trunc__())},
                        "ExpirationTime": {"N": str(time.time().__trunc__() + 300)},},
                    "provisioned_throughput" : {
                        "ReadCapacityUnits"  : 10,
                        "WriteCapacityUnits" : 10},
                    "stream_specification"   : {
                        "StreamEnabled"      : False},
                    "ttlSpecification"       : {
                        'Enabled': True,
                        'AttributeName': 'ExpirationTime'},},

                   # worker group
                   {"table_name"    : "JobWorkerGroup",
                    "key_schema"    : [{"AttributeName"       : "Groupid",
                                        "KeyType"             : "HASH"},],
                    "table_attr"     : [{"AttributeName"       : "Groupid",
                                         "AttributeType"       : "S"},],
                    "table_schema"  : {
                        "Groupid"      : {"S": "1.1.1.1"},
                        "Grouptype"    : {"S": "ec2"},       # ec2|other
                        "Groupstatus"  : {"S": "enable"},    # enable|disable
                        "Updatetime"    : {"N": str(time.time().__trunc__())},
                        "ExpirationTime": {"N": str(time.time().__trunc__() + 300)},},
                    "provisioned_throughput" : {
                        "ReadCapacityUnits"  : 10,
                        "WriteCapacityUnits" : 10},
                    "stream_specification"   : {
                        "StreamEnabled"      : False},
                    "ttlSpecification"       : {
                        'Enabled': True,
                        'AttributeName': 'ExpirationTime'},},]

queue_name      = "scheduler_job_queue"
queue_attribute = {"VisibilityTimeout": "43200",
                   "Policy"           : """{
                        "Version": "2012-10-17",
                        "Statement":[{
                          "Effect":"Allow",
                          "Principal": "*",
                          "Action":"sqs:*",
                          "Resource":"*"
                        }]
                        }""",}

bucket_name     = "boto3s3example"

auto_scaling_group_name    = "worker_group"
auto_scaling_launch_config = {
    "LaunchConfigurationName"  : "worker_group_launch_config",
    "ImageId"                  : "ami-0c2aba6c", #us-west-2 use this image
    # "ImageId"                 : "ami-571e3c30", #ap-northwest-1 use this image
    "KeyName"                  : "example",
    "SecurityGroups"           : ["sg-df4e70a4",],
    "InstanceType"             : "t2.nano",
    "InstanceMonitoring"       : {'Enabled': False},
    "EbsOptimized"             : False,
    "AssociatePublicIpAddress" : False,
}


auto_scaling_group_config = {
    "AutoScalingGroupName"     : "workergroup",
    "LaunchConfigurationName"  : "worker_group_launch_config",
    "MinSize"                  : 1,
    "MaxSize"                  : 2,
    "DesiredCapacity"          : 1,
    "HealthCheckType"          : "EC2",
    "VPCZoneIdentifier"        : "subnet-1e125857",
    #"VPCZoneIdentifier"        : "subnet-7920c030",
    "NewInstancesProtectedFromScaleIn" : False,

    "Policies":[
        {"AutoScalingGroupName"   : "workergroup",
         "PolicyName"             : "sqspolicy_create",
         "PolicyType"             : "SimpleScaling",
         "AdjustmentType"         : "ChangeInCapacity",
         "ScalingAdjustment"      : 1,
         "Cooldown"               : 300,},
        {"AutoScalingGroupName"   : "workergroup",
         "PolicyName"             : "sqspolicy_terminate",
         "PolicyType"             : "SimpleScaling",
         "AdjustmentType"         : "ChangeInCapacity",
         "ScalingAdjustment"      : -1,
         "Cooldown"               : 300,},
    ]
}

KINESIS_CONFIG = {"StreamName" : "test",
                  "ShardCount" : 1,
                 }


def create_kinesis():
    """create kinesis stream"""
    global KINESIS_CONFIG
    client = boto3.client("kinesis", region_name=default_region)
    streamobj = None
    try:
        response = client.describe_stream(StreamName=KINESIS_CONFIG["StreamName"])
        streamobj = response["StreamDescription"]
    except:
        print traceback.format_exc()

    if streamobj is None:
        client.create_stream(StreamName=KINESIS_CONFIG["StreamName"],
                             ShardCount=KINESIS_CONFIG["ShardCount"],)


def create_dynamo_table(queue_url):
    """"create dynamo table"""
    global table_list
    global default_region

    client = boto3.client("dynamodb", region_name=default_region)

    for table_desc in table_list:
        # check if dynamodb is exists
        tableobj = None
        try:
            tableobj = client.describe_table(TableName=table_desc["table_name"])
        except:
            print traceback.format_exc()

        # create dynamodb
        if tableobj is None:
            newtable = client.create_table(
                TableName=table_desc["table_name"],
                KeySchema=table_desc["key_schema"],
                AttributeDefinitions=table_desc["table_attr"],
                ProvisionedThroughput=table_desc["provisioned_throughput"],
                StreamSpecification=table_desc["stream_specification"],
            )
            tableobj = newtable

            # wait until the table is ready
            while True:
                tableobj = client.describe_table(TableName=table_desc["table_name"])
                if tableobj["Table"]["TableStatus"] == "ACTIVE":
                    break
                time.sleep(2)

            # if table name is JobScheuler ，we need to update the value of jobqueue
            if table_desc["table_name"] == "JobScheuler":
                table_desc["table_schema"]["Jobqueue"]["S"] = queue_url

            # insert sample data，to determine the schema
            client.put_item(
                TableName=table_desc["table_name"],
                Item=table_desc["table_schema"],)

            # enable ttl
            if table_desc["ttlSpecification"]:
                client.update_time_to_live(
                    TableName=table_desc["table_name"],
                    TimeToLiveSpecification=table_desc["ttlSpecification"]
                )


def create_sqs():
    """create simple queue service"""
    global queue_name
    global queue_policy

    queue_url = None
    client = boto3.client("sqs", region_name=default_region)

    # check whether the queue exists
    try:
        response = client.get_queue_url(
            QueueName=queue_name,
        )
        queue_url = response["QueueUrl"]
    except:
        print traceback.format_exc()

    if queue_url is None:
        response = client.create_queue(
            QueueName=queue_name,
            Attributes=queue_attribute,
        )
        queue_url = response["QueueUrl"]

    return queue_url


def create_s3_bucket():
    """create s3 bucket"""
    global bucket_name
    global default_region

    bucket_meta = None
    client = boto3.client("s3", region_name=default_region)
    print client.meta.endpoint_url
    try:
        bucket_meta = client.head_bucket(Bucket=bucket_name)
    except:
        print traceback.format_exc()

    if bucket_meta is None:
        bucket_meta = client.create_bucket(
            ACL='private',
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint" : default_region,
            }
        )
    return bucket_meta


def create_auto_scaling():
    global auto_scaling_launch_config
    global auto_scaling_group_config

    client = boto3.client("autoscaling", region_name=default_region)
    result = {}

    launch_configure = client.describe_launch_configurations(
        LaunchConfigurationNames=[auto_scaling_launch_config["LaunchConfigurationName"],],
    )
    if len(launch_configure["LaunchConfigurations"]) == 0:
        client.create_launch_configuration(
            LaunchConfigurationName=auto_scaling_launch_config["LaunchConfigurationName"],
            ImageId=auto_scaling_launch_config["ImageId"],
            KeyName=auto_scaling_launch_config["KeyName"],
            SecurityGroups=auto_scaling_launch_config["SecurityGroups"],
            InstanceType=auto_scaling_launch_config["InstanceType"],
            InstanceMonitoring=auto_scaling_launch_config["InstanceMonitoring"],
            EbsOptimized=auto_scaling_launch_config["EbsOptimized"],
            AssociatePublicIpAddress=auto_scaling_launch_config["AssociatePublicIpAddress"],
        )
        launch_configure = client.describe_launch_configurations(
            LaunchConfigurationNames=[auto_scaling_launch_config["LaunchConfigurationName"],],
        )

    result["launch_configuration"] = launch_configure["LaunchConfigurations"][0]

    auto_scaling_group = client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[auto_scaling_group_config["AutoScalingGroupName"],],
    )
    if len(auto_scaling_group["AutoScalingGroups"]) == 0:
        client.create_auto_scaling_group(
            AutoScalingGroupName=auto_scaling_group_config["AutoScalingGroupName"],
            LaunchConfigurationName=auto_scaling_group_config["LaunchConfigurationName"],
            MinSize=auto_scaling_group_config["MinSize"],
            MaxSize=auto_scaling_group_config["MaxSize"],
            DesiredCapacity=auto_scaling_group_config["DesiredCapacity"],
            HealthCheckType=auto_scaling_group_config["HealthCheckType"],
            VPCZoneIdentifier=auto_scaling_group_config["VPCZoneIdentifier"],
            NewInstancesProtectedFromScaleIn=
            auto_scaling_group_config["NewInstancesProtectedFromScaleIn"],
        )
        auto_scaling_group = client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[auto_scaling_group_config["AutoScalingGroupName"],],
        )
    result["auto_scaling_group"] = auto_scaling_group["AutoScalingGroups"][0]

    result["scaling_policy"] = {}
    scaling_policy = client.describe_policies(
        AutoScalingGroupName=auto_scaling_group_config["AutoScalingGroupName"],
        PolicyNames=["sqspolicy_create"],
    )
    if len(scaling_policy["ScalingPolicies"]) == 0:
        client.put_scaling_policy(
            AutoScalingGroupName='workergroup',
            PolicyName='sqspolicy_create',
            PolicyType='SimpleScaling',
            AdjustmentType='ChangeInCapacity',
            ScalingAdjustment=1,
            Cooldown=300
        )
        scaling_policy = client.describe_policies(
            AutoScalingGroupName="workergroup",
            PolicyNames=["sqspolicy_create"],
        )
    result["scaling_policy"]["sqspolicy_create"] = scaling_policy["ScalingPolicies"][0]

    scaling_policy = client.describe_policies(
        AutoScalingGroupName="workergroup",
        PolicyNames=["sqspolicy_terminate"],
    )
    if len(scaling_policy["ScalingPolicies"]) == 0:
        client.put_scaling_policy(
            AutoScalingGroupName='workergroup',
            PolicyName='sqspolicy_terminate',
            PolicyType='SimpleScaling',
            AdjustmentType='ChangeInCapacity',
            ScalingAdjustment=-1,
            Cooldown=300
        )
        scaling_policy = client.describe_policies(
            AutoScalingGroupName="workergroup",
            PolicyNames=["sqspolicy_terminate"],
        )
    result["scaling_policy"]["sqspolicy_terminate"] = scaling_policy["ScalingPolicies"][0]

    return result


def create_cloud_watch(alarm_name, comparison_operator, threshold, actions):
    """create a sqs monitor and trigger autoscalling"""
    client = boto3.client('cloudwatch', region_name=default_region)
    result = client.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator=comparison_operator,
        EvaluationPeriods=1,
        MetricName='ApproximateNumberOfMessagesVisible',
        Namespace='AWS/SQS',
        Period=60,
        Statistic='Maximum',
        Threshold=threshold,
        ActionsEnabled=True,
        AlarmActions=actions,
        AlarmDescription='Alarm when sqs message number exceeds 100',
        Dimensions=[{
            'Name'  : 'QueueName',
            'Value' : 'scheduler_job_queue'
        },],
        TreatMissingData="notBreaching",
        Unit='Seconds'
    )
    return result





def main():
    sts = boto3.client("sts", region_name=default_region)
    accountid = sts.get_caller_identity()["Account"]

    queue_url = create_sqs()
    print queue_url
    print create_dynamo_table(queue_url)
    print create_s3_bucket()
    result = create_auto_scaling()
    print create_cloud_watch("Alerm_Visible_Message_Number_create_node",
        "GreaterThanThreshold", 10,
        [result["scaling_policy"]["sqspolicy_create"]["PolicyARN"]])

    print create_cloud_watch("Alerm_Visible_Message_Number_terminate_node",
        "LessThanThreshold", 5,
        [result["scaling_policy"]["sqspolicy_terminate"]["PolicyARN"]])


if __name__ == "__main__":
    main()
