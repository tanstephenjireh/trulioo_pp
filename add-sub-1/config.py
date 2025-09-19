import boto3
from functools import lru_cache

ssm = boto3.client("ssm")

@lru_cache()
def get_ssm_param(name, with_decryption=True):
    response = ssm.get_parameter(
        Name=name,
        WithDecryption=with_decryption
    )
    return response['Parameter']['Value']
