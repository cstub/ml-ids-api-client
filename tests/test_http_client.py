import pytest
import os
import responses
import pandas as pd
from pandas.util.testing import assert_frame_equal
from ml_ids_api_client.conf import TEST_DATA_DIR
from ml_ids_api_client.http_client import call_predict_api

ML_IDS_URL = 'http://api.ml-ids.com/invocations'


@pytest.fixture
def test_data():
    validation_data_path = os.path.join(TEST_DATA_DIR, 'dataset.h5')
    return pd.read_hdf(validation_data_path)[:1]


@pytest.fixture
def api_success_response():
    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=ML_IDS_URL,
                 json=[0.0],
                 status=200)
        yield rsps


@pytest.fixture
def api_server_error_response():
    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=ML_IDS_URL,
                 json={'error': 'server-error'},
                 status=500)
        yield rsps


@pytest.fixture
def api_client_error_response():
    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=ML_IDS_URL,
                 json={'error': 'client-error'},
                 status=400)
        yield rsps


def test_call_predict_api_must_invoke_api_endpoint(test_data, api_success_response):
    call_predict_api(ML_IDS_URL, test_data)

    assert len(api_success_response.calls) == 1


def test_call_predict_api_must_set_content_type_json_pandas(test_data, api_success_response):
    call_predict_api(ML_IDS_URL, test_data)

    assert len(api_success_response.calls) == 1
    assert api_success_response.calls[0].request.headers['Content-Type'] == 'application/json; format=pandas-split'


def test_call_predict_api_must_send_body_in_pandas_json_split_format(test_data, api_success_response):
    call_predict_api(ML_IDS_URL, test_data)

    assert len(api_success_response.calls) == 1

    expected = test_data.drop(columns=['label'])

    request_body = api_success_response.calls[0].request.body
    body_as_df = pd.read_json(request_body, orient='split', convert_dates=False)

    assert_frame_equal(expected.reset_index(drop=True),
                       body_as_df.reset_index(drop=True),
                       check_dtype=False)


def test_call_predict_api_must_return_predictions_on_success(test_data, api_success_response):
    predictions = call_predict_api(ML_IDS_URL, test_data)

    assert len(api_success_response.calls) == 1
    assert predictions == [0.0]


def test_call_predict_api_must_raise_error_on_server_error(test_data, api_server_error_response):
    with pytest.raises(IOError):
        call_predict_api(ML_IDS_URL, test_data)


def test_call_predict_api_must_raise_error_on_client_error(test_data, api_client_error_response):
    with pytest.raises(IOError):
        call_predict_api(ML_IDS_URL, test_data)
