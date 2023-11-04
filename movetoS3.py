import pandas as pd
import boto3
from botocore.exceptions import ClientError
import os
import ast


# THIS IS NOT BEST PRACTICE JUST FOR DEMO - QUICK N DIRTY
# authenticate into aws -- security use CLI, secrets manager, etc.
keys = pd.read_csv("ginomain_accessKeys.csv")
access_key = keys['Access key ID'][0]
secret_key = keys['Secret access key'][0]
# access_key, secret_key


# THIS IS CLOSER TO A GOOD PRACTICE BUT STILL NOT IDEAL --
# research secrets manager
# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/
def get_secret():

    secret_name = "accessKeySecretKeyPair"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret

    # Your code goes here.


secretsDict = get_secret()

secretsDict = ast.literal_eval(secretsDict)


key = list(secretsDict.keys())[0]
secret = secretsDict[key]

# load specific files into an s3 bucket from a directory called partitions
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

for file in os.listdir("partitions"):
    print("Writing")
    s3.upload_file(f"partitions/{file}", # the file we're uploading
                'databricks-workspace-stack-b17b9-metastore-bucket', # the name of the s3 bucket were uploading to
                  f"partitions/{file}") # the name of the file we want to give it in s3
    print("Written")




# list all files in an s3 bucket
s3_ = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
bucket = s3_.Bucket('databricks-workspace-stack-b17b9-metastore-bucket')
for obj in bucket.objects.all():
    print(obj.key)

