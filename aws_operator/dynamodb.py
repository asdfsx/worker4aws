"""
Functions used to operate dynamodb
"""

import types

def updateworker(client, current_time, worker_id, worker_status, worker_job):
    """update worker status
    """
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

def updatejobstage(client, job_id, old_job_stage, new_job_stage):
    """update job stage
    """
    condition_expression="#js = :ojs"
    expression_attribute_values = {":ojs": {"S" : old_job_stage},
                                   ":njs": {"S" : new_job_stage},}
    if isinstance(old_job_stage, types.ListType):
        expression_attribute_values = {":njs": {"S" : new_job_stage},}
        for index, stage in enumerate(old_job_stage):
            key = ":ojs%02d" % index
            expression_attribute_values[key] = {"S" : stage}
            if index == 0:
                condition_expression = "#js = %s" % key
            else:
                condition_expression += " OR #js = %s" % key

    client.update_item(
        ExpressionAttributeNames={
            "#js": "Jobstage",},
        ExpressionAttributeValues=expression_attribute_values,
        Key={"Jobid": {"S": job_id},},
        ReturnValues='NONE',
        TableName="JobScheuler",
        ConditionExpression=condition_expression,
        UpdateExpression="SET #js = :njs",
    )
