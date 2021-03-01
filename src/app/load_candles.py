import logging
import os

from cbt.cb import CoinbaseClient
from cbt.pg import PostgresClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


if __name__ == "__main__":

    host = os.getenv("PG_HOST")
    database = os.getenv("PG_DATABASE")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    lookback = int(os.getenv("LOOKBACK", 1))
    cb_client = CoinbaseClient()

    for product_id in (
        "BTC-USD",
        "ETH-USD",
        "LTC-USD",
        "BCH-USD",
        "EOS-USD",
        "DASH-USD",
        "OXT-USD",
        "MKR-USD",
        "XLM-USD",
        "ATOM-USD",
    ):
        cb_client.download_candles(
            f"/tmp/candles_{product_id}.csv", product_id=product_id, lookback=lookback
        )
        pg_client = PostgresClient(host, database, user, password)
        pg_client.execute_sql("/app/sql/create_objects.sql")
        pg_client.copy_from(f"/tmp/candles_{product_id}.csv", "landing.candles")
        pg_client.execute_sql("/app/sql/load_ods_candles.sql")
