import boto3
from botocore.exceptions import ClientError
import os
import sys

s3 = boto3.resource('s3')
session = boto3.session.Session()
region = session.region_name
s3client = boto3.client('s3', region_name = region)
bucket_name = ""

def checkBucket(bucket):
    try:
        s3client.head_bucket(Bucket = bucket)
        return True

    except ClientError as e:
        return False

def restore(bucket, bucket_directory, directory):
    for obj in bucket.objects.filter(Prefix = directory):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        
        bucket.download_file(obj.key, obj.key)

def main():
    names = sys.argv[1].split('::')
    global bucket_name 
    bucket_name = names[0]
    bucket_directory = names[1]

    directory_name = sys.argv[2]

    exists = checkBucket(bucket_name)
    if not exists:
        return

    bucket = s3.Bucket(bucket_name)
    restore(bucket, bucket_directory, directory_name)