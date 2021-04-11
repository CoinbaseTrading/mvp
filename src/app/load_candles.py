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
    days_to_load = int(os.getenv("DAYS_TO_LOAD", 1))

    cb_client = CoinbaseClient()
    pg_client = PostgresClient(host, database, user, password)
    pg_client.execute_sql("/app/sql/create_objects.sql")

    for product_id in cb_client.yield_products("USD"):
        cb_client.download_candles(
            f"/tmp/candles_{product_id}.csv", product_id=product_id, days_to_load=days_to_load
        )
        pg_client.copy_from(f"/tmp/candles_{product_id}.csv", "landing.candles")

    pg_client.execute_sql("/app/sql/load_ods_candles.sql")
