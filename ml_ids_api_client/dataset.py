import pandas as pd
import re
import boto3
import os
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config


def load_dataset(path: str, s3_region: str, s3_storage_path: str) -> pd.DataFrame:
    if path.startswith(r'file://'):
        return load_dataset_from_file(re.sub('file://', '', path))
    if path.startswith(r's3://'):
        return load_dataset_from_s3(re.sub('s3://', '', path), s3_region, s3_storage_path)

    raise ValueError('Invalid dataset URI provided! Supported protocols are ["file://", "s3://"]')


def load_dataset_from_file(path: str) -> pd.DataFrame:
    dataset = pd.read_hdf(path)
    return dataset


def load_dataset_from_s3(path: str, s3_region: str, s3_storage_path: str) -> pd.DataFrame:
    if os.path.isfile(s3_storage_path):
        print('Dataset is already present on path ["{}"].'.format(s3_storage_path))
    else:
        print('Downloading dataset from S3 location ["{}"] to ["{}"]'.format(path, s3_storage_path))
        transfer_file_from_s3(path, s3_region, s3_storage_path)

    return pd.read_hdf(s3_storage_path)


def transfer_file_from_s3(path: str, s3_region: str, s3_storage_path: str) -> None:
    try:
        storage_dir = os.path.sep.join(s3_storage_path.split(os.path.sep)[:-1])
        os.makedirs(storage_dir, exist_ok=True)

        bucket = path.split(os.path.sep)[0]
        key = os.path.sep.join(path.split(os.path.sep)[1:])
        s3 = boto3.client('s3',
                          region_name=s3_region,
                          config=Config(signature_version=UNSIGNED))
        s3.download_file(bucket, key, s3_storage_path)
    except ClientError as err:
        if err.response['Error']['Code'] == '404':
            raise FileNotFoundError('File ["s3://{}"] not found.'.format(path))
        else:
            raise IOError('File ["s3://{}"] could not be retrieved. Cause: {}.'.format(path, err))
