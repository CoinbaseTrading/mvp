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
    product_id = os.getenv("PRODUCT_ID", "BTC-USD")
    lookback = int(os.getenv("LOOKBACK", 1))

    cb_client = CoinbaseClient()
    cb_client.download_candles("/tmp/candles.csv", product_id=product_id, lookback=lookback)

    pg_client = PostgresClient(host, database, user, password)
    pg_client.execute_sql("/app/sql/create_objects.sql")
    pg_client.copy_from("/tmp/candles.csv", "landing.candles")
    pg_client.execute_sql("/app/sql/load_ods_candles.sql")
