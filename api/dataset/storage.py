import boto3
from api.settings import default_settings
from typing import Any


def initialize_client() -> Any:
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=default_settings.minio_user,
        aws_secret_access_key=default_settings.minio_pass,
        endpoint_url=default_settings.minio_url,
    )
    if len(client.list_buckets()) == 0:
        client.create_bucket(Bucket=default_settings.bucket_name)
    return client
