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


def checkgroupstatus(client, groupid):
    """check group status by groupid"""
    result = client.get_item(
        TableName='JobWorkerGroup',
        Key={"Groupid" : {"S" : groupid}},
    )
    if "Item" in result and "Groupstatus" in result["Item"]:
        return result["Item"]["Groupstatus"]["S"]
    else:
        return ""



def updategroup(client, current_time, group_id, group_type, group_status):
    """update group status"""
    expression_attributenames = {"#gt": "Grouptype",
                                 "#ut": "Updatetime",
                                 "#et": "ExpirationTime",}
    expression_attributevalues = {":gt": {"S" : group_type},
                                  ":ut": {"N" : str(current_time)},
                                  ":et": {"N" : str(current_time + 600)},}
    update_expression = 'SET #gt = :gt, #ut = :ut, #et = :et'
    if group_status != "":
        expression_attributenames["#gs"] = "Groupstatus"
        expression_attributevalues[":gs"] = {"S" : group_status}
        update_expression = 'SET #gt = :gt, #gs = :gs, #ut = :ut, #et = :et'

    client.update_item(
        ExpressionAttributeNames=expression_attributenames,
        ExpressionAttributeValues=expression_attributevalues,
        Key={"Groupid": {"S": group_id},},
        ReturnValues='NONE',
        TableName="JobWorkerGroup",
        UpdateExpression=update_expression,
    )


def updatejobstage(client, job_id, old_job_stage, new_job_stage, job_result):
    """update job stage
    """
    condition_expression="#js = :ojs"
    expression_attribute_values = {":ojs": {"S" : old_job_stage},
                                   ":njs": {"S" : new_job_stage},
                                   ":jr" : {"S" : job_result},}
    if isinstance(old_job_stage, types.ListType):
        expression_attribute_values = {":njs": {"S" : new_job_stage},
                                       ":jr" : {"S" : job_result},}
        for index, stage in enumerate(old_job_stage):
            key = ":ojs%02d" % index
            expression_attribute_values[key] = {"S" : stage}
            if index == 0:
                condition_expression = "#js = %s" % key
            else:
                condition_expression += " OR #js = %s" % key

    client.update_item(
        ExpressionAttributeNames={
            "#js": "Jobstage",
            "#jr": "Jobresult",},
        ExpressionAttributeValues=expression_attribute_values,
        Key={"Jobid": {"S": job_id},},
        ReturnValues='NONE',
        TableName="JobScheuler",
        ConditionExpression=condition_expression,
        UpdateExpression="SET #js = :njs, #jr = :jr",
    )

def getjobbyid(client, job_id):
    """check whether job_id exists"""
    result = client.get_item(TableName="JobScheuler",
                             Key={"Jobid": {"S": job_id},},)
    if "Item" in result:
        return result["Item"]
    else:
        return None
