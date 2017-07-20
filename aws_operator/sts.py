import boto3

sts_client = boto3.client("sts")

def get_account_id():
    """get aws account id
    """
    accountid = sts_client.get_caller_identity()["Account"]
    return accountid
