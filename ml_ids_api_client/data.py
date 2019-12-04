import pandas as pd
from collections import namedtuple
from typing import List, Union, Tuple, Dict

Selection = namedtuple('Selection', ['category', 'nr_samples', 'delay'])
RandomSelection = namedtuple('RandomSelection', ['nr_samples', 'delay'])


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
    if len(ds) > selection.nr_samples:
        return ds.sample(n=selection.nr_samples)
    return ds


def select_category_samples(ds: pd.DataFrame, selection: Selection) -> pd.DataFrame:
    samples = ds[ds.label == selection.category]
    if len(samples) > selection.nr_samples:
        return samples.sample(n=selection.nr_samples)
    return samples


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
