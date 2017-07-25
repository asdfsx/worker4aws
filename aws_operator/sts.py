import boto3

def get_account_id(client):
    """get aws account id
    """
    accountid = client.get_caller_identity()["Account"]
    return accountid
