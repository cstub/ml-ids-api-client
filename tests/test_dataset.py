import pytest
import os
import tempfile
from ml_ids_api_client.conf import TEST_DATA_DIR
from ml_ids_api_client.dataset import load_dataset


def test_load_dataset_must_raise_ValueError_if_protocol_not_supported():
    path = os.path.join(TEST_DATA_DIR, 'dataset.h5')

    with pytest.raises(ValueError):
        load_dataset(path, 'tmp.h5')


def test_load_dataset_from_file_must_load_data_if_present():
    path = os.path.join(TEST_DATA_DIR, 'dataset.h5')
    dataset = load_dataset('file://' + path, 'tmp.h5')

    assert len(dataset) == 1000


def test_load_dataset_from_file_must_raise_FileNotFoundError_if_file_does_not_exist():
    path = os.path.join(TEST_DATA_DIR, 'not_existent.h5')

    with pytest.raises(FileNotFoundError):
        load_dataset('file://' + path, 'tmp.h5')


def test_load_dataset_from_s3_must_load_data_if_present():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, 'dataset.h5')
        dataset = load_dataset('s3://ml-ids-2018-sm/testing/test.h5', path)

        assert len(dataset) == 1000


def test_load_dataset_from_s3_must_raise_FileNotFoundError_if_file_does_not_exist():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, 'dataset.h5')

        with pytest.raises(FileNotFoundError):
            load_dataset('s3://ml-ids-2018-sm/testing/non_existent.h5', path)
