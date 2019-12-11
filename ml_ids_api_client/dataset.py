"""
Utility functions to read datasets.
"""
import os
import re
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config


def load_dataset(path: str, s3_region: str, s3_storage_path: str) -> pd.DataFrame:
    """
    Loads a dataset in `h5` format from the given path.

    :param path: Either a local file (`file://`) or a S3 bucket (`s3://`).
    :param s3_region: The AWS region if the path is an S3 bucket.
    :param s3_storage_path: The local storage path for the downloaded file from the S3 bucket.
    :return: Dataset as a Pandas DataFrame.
    """
    if path.startswith(r'file://'):
        return load_dataset_from_file(re.sub('file://', '', path))
    if path.startswith(r's3://'):
        return load_dataset_from_s3(re.sub('s3://', '', path), s3_region, s3_storage_path)

    raise ValueError('Invalid dataset URI provided! Supported protocols are ["file://", "s3://"]')


def load_dataset_from_file(path: str) -> pd.DataFrame:
    """
    Loads a dataset in `h5` format from a local file path.

    :param path: Path to the local file.
    :return: Dataset as a Pandas DataFrame.
    """
    dataset = pd.read_hdf(path)
    return dataset


def load_dataset_from_s3(path: str, s3_region: str, s3_storage_path: str) -> pd.DataFrame:
    """
    Loads a dataset in `h5` format from an S3 bucket.

    :param path: Path of the file in the S3 bucket.
    :param s3_region: The AWS region if the path is an S3 bucket.
    :param s3_storage_path: The local storage path for the downloaded file from the S3 bucket.
    :return: Dataset as a Pandas DataFrame.
    """
    if os.path.isfile(s3_storage_path):
        print('Dataset is already present on path ["{}"].'.format(s3_storage_path))
    else:
        print('Downloading dataset from S3 location ["{}"] to ["{}"]'.format(path, s3_storage_path))
        transfer_file_from_s3(path, s3_region, s3_storage_path)

    return pd.read_hdf(s3_storage_path)


def transfer_file_from_s3(path: str, s3_region: str, s3_storage_path: str) -> None:
    """
    Downloads a file from an S3 bucket and stores it as a local file.

    :param path: Path of the file in the S3 bucket.
    :param s3_region: The AWS region if the path is an S3 bucket.
    :param s3_storage_path: The local storage path for the downloaded file from the S3 bucket.
    :return: None
    """
    try:
        storage_dir = os.path.sep.join(s3_storage_path.split(os.path.sep)[:-1])
        os.makedirs(storage_dir, exist_ok=True)

        bucket = path.split(os.path.sep)[0]
        key = os.path.sep.join(path.split(os.path.sep)[1:])
        s3_client = boto3.client('s3',
                                 region_name=s3_region,
                                 config=Config(signature_version=UNSIGNED))
        s3_client.download_file(bucket, key, s3_storage_path)
    except ClientError as err:
        if err.response['Error']['Code'] == '404':
            raise FileNotFoundError('File ["s3://{}"] not found.'.format(path))
        raise IOError('File ["s3://{}"] could not be retrieved. Cause: {}.'.format(path, err))
