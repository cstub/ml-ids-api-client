"""
HTTP utilities to invoke the `predict` endpoint of the ML-IDS API.
"""
from typing import List
import urllib.parse
import pandas as pd
import requests
from requests.exceptions import HTTPError

API_REQUEST_HEADERS = {
    'Content-Type': 'application/json; format=pandas-split'
}
API_ENDPOINT_NAME = '/api/predictions'


def call_predict_api(url: str, data: pd.DataFrame) -> List[float]:
    """
    Invokes the `predict` endpoint of the ML-IDS API.

    :param url: URL of the API.
    :param data: Features to send in request body.
    :return: List of predictions in the range of `[0, 1]`. Returns one prediction per input row in the same order.
    """
    json_body = data.drop(columns=['label']).to_json(orient='split', index=False)

    try:
        response = requests.post(url=urllib.parse.urljoin(url, API_ENDPOINT_NAME),
                                 data=json_body,
                                 headers=API_REQUEST_HEADERS)

        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        raise IOError('{} - {}'.format(http_err, response.text))
