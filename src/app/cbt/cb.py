import logging

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class CoinbaseClient:
    def __init__(self):
        pass

    def get_candles(
        self, start_time, end_time, product_id="BTC-USD", granularity=60,
    ):

        valid = [60, 300, 900, 3600, 21600, 86400]
        if granularity not in valid:
            raise ValueError(
                f"Granularity must be one of the following values {' '.join(valid)}"
            )

        logging.info(
            f"Getting {product_id} data for interval {start_time} to {end_time}..."
        )

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429])
        s.mount("https://", HTTPAdapter(max_retries=retries))

        response = s.get(
            f"https://api.pro.coinbase.com/products/{product_id}/candles",
            params={
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "granularity": granularity,
            },
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.status_code)

    @staticmethod
    def yield_products(suffix):

        response = requests.get("https://api.pro.coinbase.com/products")
        if response.status_code == 200:
            for d in response.json():
                if d.get("id").endswith(suffix):
                    yield d.get("id")
        else:
            raise Exception(response.status_code)
