import pandas as pd
import re
import boto3
import os
from collections import namedtuple
from typing import List, Union, Tuple, Dict

Selection = namedtuple('Selection', ['category', 'nr_samples', 'delay'])
RandomSelection = namedtuple('RandomSelection', ['nr_samples', 'delay'])


def load_dataset(path: str, storage_path: str) -> pd.DataFrame:
    if path.startswith(r'file://'):
        return load_dataset_from_file(re.sub('file://', '', path))
    if path.startswith(r's3://'):
        return load_dataset_from_s3(re.sub('s3://', '', path), storage_path)

    raise ValueError('Invalid dataset URI provided! Supported types are ["file://", "s3://"]')


def load_dataset_from_file(path: str) -> pd.DataFrame:
    dataset = pd.read_hdf(path)
    return dataset


def load_dataset_from_s3(path: str, storage_path: str) -> pd.DataFrame:
    if os.path.isfile(storage_path):
        print('Dataset is already present on path ["{}"].'.format(storage_path))
    else:
        print('Downloading dataset from S3 location ["{}"] to ["{}"]'.format(path, storage_path))
        storage_dir = os.path.sep.join(storage_path.split(os.path.sep)[:-1])
        os.makedirs(storage_dir, exist_ok=True)

        bucket = path.split(os.path.sep)[0]
        key = os.path.sep.join(path.split(os.path.sep)[1:])
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, storage_path)

    return pd.read_hdf(storage_path)


def get_categories(ds: pd.DataFrame) -> Dict[int, str]:
    categories = ds.label.value_counts().index.to_list()
    return dict(zip(range(0, len(categories)), categories))


def select_samples(ds: pd.DataFrame, selection: Union[Selection, RandomSelection]) -> pd.DataFrame:
    if isinstance(selection, Selection):
        return select_category_samples(ds, selection)
    elif isinstance(selection, RandomSelection):
        return select_random_samples(ds, selection)
    else:
        raise ValueError('Invalid selection {} given. Selection must be of type [Selection | RandomSelection].'
                         .format(selection))


def select_random_samples(ds: pd.DataFrame, selection: RandomSelection) -> pd.DataFrame:
    return ds.sample(n=selection.nr_samples)


def select_category_samples(ds: pd.DataFrame, selection: Selection) -> pd.DataFrame:
    return ds[ds.label == selection.category].sample(n=selection.nr_samples)


def merge_predictions(samples: pd.DataFrame, predictions: List[float]) -> Tuple[pd.DataFrame, float]:
    def is_correct_prediction(row):
        label_is_attack = row['label'] != 'Benign'
        pred_is_attack = row['prediction_raw']
        return label_is_attack == pred_is_attack

    df = samples
    df['prediction_raw'] = predictions
    df['predicted_label'] = df.prediction_raw.apply(lambda p: 'Benign' if p == 0 else 'Attack')
    df['is_correct'] = df.apply(is_correct_prediction, axis=1)
    acc = (len(df[df.is_correct == True]) / len(df)) * 100

    cols = ['is_correct', 'label', 'predicted_label']
    cols = cols + df.columns.drop(cols).to_list()

    return df[cols], acc
