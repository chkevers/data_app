import boto3
from pathlib import Path


def empty_bucket(bucket):
    # boto3.set_stream_logger('')
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    bucket.objects.all().delete()


def del_bucket(bucket):
    # boto3.set_stream_logger('')
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    bucket.object_versions.delete()
    bucket.delete()

# del_bucket('tennisappck')

def create_bucket(bucket):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'})

create_bucket('tennis-app-ck')


def upload_s3(file, bucket, key):
    s3 = boto3.client('s3')
    s3.upload_file(file, bucket, key)


def list_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets().get('Buckets')
    return print(response)


def list_objects(bucket, key):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        objs = list(bucket.objects.filter(Prefix=key))
        ex_objs = sorted(set([str(f"s3://{bucket.name}/{obj.key}") for obj in objs  if str(obj.key) != key]))
        return ex_objs
