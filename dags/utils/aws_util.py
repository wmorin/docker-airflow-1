"""
    AWS interfaces
"""
import os
import boto3


def s3_upload_file(bucket_name, source_file, target_path, target_file=None):
    """Upload to AWS s3

    :param: bucket_name s3 bucket_name
    :param: source_file absolute file path
    :param: target_path path to store file in s3
    :target_file: target_file optional target file name.
    """

    s3_client = boto3.client('s3')
    file_name = source_file.split('/')[-1]
    if not target_file:
        target_file = file_name
    target_file = os.path.join(target_path, target_file)

    print(f'Upload {source_file} to {target_file} in {bucket_name} bucket in s3.')
    s3_client.upload_file(source_file, bucket_name, target_file)


def s3_copy_file(source_bucket, source_key, dest_bucket, dest_key):
    """Copy a file to another location in s3

    :param: source_bucket bucket name of source key
    :param: source_key source key
    :param: dest_bucket bucket name of destination key
    :param: dest_key destination key
    """

    s3_client = boto3.client('s3')

    source = {
        'Bucket': source_bucket,
        'Key': source_key
    }

    print(f'source::{source} to {dest_key} in {dest_bucket}')

    s3_client.copy(source, dest_bucket, dest_key)


def s3_download_file(bucket_name, source_key, path_to_store):
    print(f'Download::{source_key} in {bucket_name} to {path_to_store}')
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, source_key, path_to_store)


def s3_file_exists(bucket_name, source_key):
    print(f'Does Exist::{source_key} in {bucket_name}')
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=source_key,
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == source_key:
            return True

    return False


def create_etl_daily_key_prefix(date):
    return f'etl/daily/{date}'
