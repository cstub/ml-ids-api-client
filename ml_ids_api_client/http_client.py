import pandas as pd
import requests
import urllib.parse
from requests.exceptions import HTTPError
from typing import List

API_REQUEST_HEADERS = {
    'Content-Type': 'application/json; format=pandas-split'
}
API_ENDPOINT_NAME = 'invocations'


def call_predict_api(url: str, data: pd.DataFrame) -> List[float]:
    json_body = data.drop(columns=['label']).to_json(orient='split', index=False)

    try:
        response = requests.post(url=urllib.parse.urljoin(url, API_ENDPOINT_NAME),
                                 data=json_body,
                                 headers=API_REQUEST_HEADERS)

        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        raise IOError('{} - {}'.format(http_err, response.text))
