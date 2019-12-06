import pytest
import os
import pandas as pd
from tests.conf import TEST_DATA_DIR
from ml_ids_api_client.data import get_categories, select_samples, Selection, RandomSelection

SAMPLE_COUNT = 100


@pytest.fixture
def test_data():
    validation_data_path = os.path.join(TEST_DATA_DIR, 'dataset.h5')
    return pd.read_hdf(validation_data_path)[:SAMPLE_COUNT]


def test_get_categories_must_return_dictionary_of_categories(test_data):
    categories = get_categories(test_data)

    assert len(categories) == 7
    assert list(categories.keys()) == list(range(0, 7))
    assert 'Benign' in categories.values()
    assert 'DDoS attacks-LOIC-HTTP' in categories.values()
    assert 'DoS attacks-SlowHTTPTest' in categories.values()
    assert 'DoS attacks-GoldenEye' in categories.values()
    assert 'FTP-BruteForce' in categories.values()
    assert 'Brute Force -Web' in categories.values()
    assert 'DDOS attack-LOIC-UDP' in categories.values()


def test_select_samples_from_selection_must_return_samples_of_given_category(test_data):
    samples = select_samples(test_data, Selection(category='Benign', nr_samples=10, delay=None))

    assert len(samples) == 10
    assert len(samples.label.drop_duplicates()) == 1
    assert samples.iloc[0]['label'] == 'Benign'


def test_select_samples_from_selection_must_return_all_samples_of_given_category(test_data):
    samples = select_samples(test_data, Selection(category='Benign', nr_samples=SAMPLE_COUNT, delay=None))

    assert len(samples) == 56


def test_select_samples_from_selection_must_return_empty_if_category_not_present(test_data):
    samples = select_samples(test_data, Selection(category='Non-Existent', nr_samples=10, delay=None))

    assert len(samples) == 0


def test_select_samples_from_random_must_return_samples_of_random_categories(test_data):
    samples = select_samples(test_data, RandomSelection(nr_samples=10, delay=None))

    assert len(samples) == 10
    assert len(samples.label.drop_duplicates()) > 1


def test_select_samples_from_random_must_return_all_samples(test_data):
    samples = select_samples(test_data, RandomSelection(nr_samples=SAMPLE_COUNT, delay=None))

    assert len(samples) == SAMPLE_COUNT
