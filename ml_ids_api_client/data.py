"""
Utility functions to manipulate datasets.
"""
from typing import List, Union, Tuple, Dict
from collections import namedtuple
import pandas as pd

Selection = namedtuple('Selection', ['category', 'nr_samples', 'delay'])
RandomSelection = namedtuple('RandomSelection', ['nr_samples', 'delay'])


def get_categories(df: pd.DataFrame) -> Dict[int, str]:
    """
    Returns unique network traffic categories from the given DataFrame.
    :param df: Pandas DataFrame.
    :return: Unique categories as dict. A unique number is assigned to each category.
    """
    categories = df.label.value_counts().index.to_list()
    return dict(zip(range(0, len(categories)), categories))


def select_samples(df: pd.DataFrame, selection: Union[Selection, RandomSelection]) -> pd.DataFrame:
    """
    Selects samples from the given DataFrame. Selection can either be performed by specifying a category (`Selection`)
    or by random sampling of instances (`RandomSelection`).

    :param df: Pandas DataFrame.
    :param selection: Selection parameters to sample instances (Selection | RandomSelection).
    :return: Selected samples as Pandas DataFrame.
    """
    if isinstance(selection, Selection):
        return select_category_samples(df, selection)
    if isinstance(selection, RandomSelection):
        return select_random_samples(df, selection)
    raise ValueError('Invalid selection {} given. Selection must be of type [Selection | RandomSelection].'
                     .format(selection))


def select_random_samples(df: pd.DataFrame, selection: RandomSelection) -> pd.DataFrame:
    """
    Selects random samples from the given DataFrame.

    :param df: Pandas DataFrame.
    :param selection: Selection parameters.
    :return: Selected samples as Pandas DataFrame.
    """
    if len(df) > selection.nr_samples:
        return df.sample(n=selection.nr_samples)
    return df


def select_category_samples(df: pd.DataFrame, selection: Selection) -> pd.DataFrame:
    """
    Selects samples of the given category from the DataFrame.

    :param df: Pandas DataFrame.
    :param selection: Selection parameters.
    :return: Selected samples as Pandas DataFrame.
    """
    samples = df[df.label == selection.category]
    if len(samples) > selection.nr_samples:
        return samples.sample(n=selection.nr_samples)
    return samples


def merge_predictions(samples: pd.DataFrame, predictions: List[float]) -> Tuple[pd.DataFrame, float]:
    """
    Merges input samples with attack predictions.

    :param samples: Pandas DataFrame containing input samples.
    :param predictions: Attack predictions.
    :return: Tuple of (merged DataFrame, Accuracy of predictions).
    """

    def is_correct_prediction(row):
        label_is_attack = row['label'] != 'Benign'
        pred_is_attack = row['prediction_raw']
        return label_is_attack == pred_is_attack

    df = samples
    df['prediction_raw'] = predictions
    df['predicted_label'] = df.prediction_raw.apply(lambda p: 'Benign' if p == 0 else 'Attack')
    df['is_correct'] = df.apply(is_correct_prediction, axis=1)
    acc = (len(df[df.is_correct]) / len(df)) * 100

    cols = ['is_correct', 'label', 'predicted_label']
    cols = cols + df.columns.drop(cols).to_list()

    return df[cols], acc
