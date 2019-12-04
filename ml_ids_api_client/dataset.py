import pandas as pd
import re
import boto3
import os
from botocore.exceptions import ClientError


def load_dataset(path: str, storage_path: str) -> pd.DataFrame:
    if path.startswith(r'file://'):
        return load_dataset_from_file(re.sub('file://', '', path))
    if path.startswith(r's3://'):
        return load_dataset_from_s3(re.sub('s3://', '', path), storage_path)

    raise ValueError('Invalid dataset URI provided! Supported protocols are ["file://", "s3://"]')


def load_dataset_from_file(path: str) -> pd.DataFrame:
    dataset = pd.read_hdf(path)
    return dataset


def load_dataset_from_s3(path: str, storage_path: str) -> pd.DataFrame:
    if os.path.isfile(storage_path):
        print('Dataset is already present on path ["{}"].'.format(storage_path))
    else:
        print('Downloading dataset from S3 location ["{}"] to ["{}"]'.format(path, storage_path))
        transfer_file_from_s3(path, storage_path)

    return pd.read_hdf(storage_path)


def transfer_file_from_s3(path: str, storage_path: str) -> None:
    try:
        storage_dir = os.path.sep.join(storage_path.split(os.path.sep)[:-1])
        os.makedirs(storage_dir, exist_ok=True)

        bucket = path.split(os.path.sep)[0]
        key = os.path.sep.join(path.split(os.path.sep)[1:])
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, storage_path)
    except ClientError as err:
        if err.response['Error']['Code'] == '404':
            raise FileNotFoundError('File ["s3://{}"] not found.'.format(path))
        else:
            raise err
