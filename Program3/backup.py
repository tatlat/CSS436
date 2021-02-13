import boto3
from botocore.exceptions import ClientError
import os
import sys
from datetime import datetime, timezone

s3 = boto3.resource('s3')
session = boto3.session.Session()
region = session.region_name
s3client = boto3.client('s3', region_name = region)
bucket_name = ""

def checkBucket(bucket):
    try:
        s3client.head_bucket(Bucket = bucket)

    except ClientError as e:
        print("Creating bucket: ", bucket)
        location = {'LocationConstraint': region}
        s3.create_bucket(Bucket = bucket, CreateBucketConfiguration = location)

def backup_file(bucket, file_path, bucket_path, modified):
    if modified:
        print("Updating file: " + file)
    else:
        print("Uploading file: " + file)
    bucket.upload_file(file_path, bucket_path)

def modified(obj, file_path):
    s3date = obj["LastModified"]
    timestamp = os.path.getmtime(file_path)
    local_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)

    return local_date > s3date


def backup_directory(directory, bucket, bucket_directory):
    try:
        path, subdirs, files = os.walk(directory).next()

        for file in files:
            file_path = os.path.join(path, file)
            bucket_path = bucket_directory + "/" + file
            try:
                obj = s3.head_object(Bucket = bucket_name, \
                                    Key = bucket_path)

                if modified(obj, file_path):
                    backup_file(bucket, file_path, bucket_path, True)
            
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    backup_file(bucket, file_path, bucket_path, False)

        for dir in subdirs:
            subdir = os.path.join(path, dir)
            bucket_subdir = bucket_directory + "/" + dir
            backup_directory(subdir, bucket, bucket_subdir)

    except StopIteration:
        print("Finished backing up " + directory)


def main():
    directory_name = sys.argv[1]
    if not os.path.exists(directory_name):
        print("The directory does not exist.")
        return
        
    names = sys.argv[2].split('::')
    global bucket_name 
    bucket_name = names[0]
    bucket_directory = names[1]

    checkBucket(bucket_name)
    bucket = s3.Bucket(bucket_name)
    backup_directory(directory_name, bucket, bucket_directory)
    
