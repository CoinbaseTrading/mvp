import csv
from datetime import datetime, timezone
import logging
from time import sleep

import requests

from cbt.utils.candles import yield_batch


class CoinbaseClient:
    def __init__(self):
        pass

    def download_candles(
        self,
        target_file,
        product_id="BTC-USD",
        granularity=60,
        lookback=1,
        delimiter="|",
        sleep_seconds=0.2,
    ):

        valid = [60, 300, 900, 3600, 21600, 86400]
        if granularity not in valid:
            raise ValueError(
                f"Granularity must be one of the following values {' '.join(valid)}"
            )

        now = datetime.now(timezone.utc)
        with open(target_file, "w") as f:
            writer = csv.writer(f, delimiter=delimiter)
            for start_time, end_time in yield_batch(now, lookback, granularity):
                logging.info(f"Getting data for interval {start_time} to {end_time}...")
                response = requests.get(
                    f"https://api.pro.coinbase.com/products/{product_id}/candles",
                    params={
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat(),
                        "granularity": granularity,
                    },
                )
                data = response.json()
                for row in data:
                    writer.writerow([product_id] + row + [now.isoformat()])

                # sleeping to avoid hitting the rate limit
                sleep(sleep_seconds)
