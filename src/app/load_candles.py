import csv
from datetime import datetime, timezone
import logging
import os

from cbt.cb import CoinbaseClient
from cbt.pg import PostgresClient
from cbt.utils.candles import yield_batch

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

    for start_time, end_time in yield_batch(datetime.now(timezone.utc), days_to_load):
        with open("/tmp/candles.csv", "w") as f:
            writer = csv.writer(f, delimiter="|")
            created_time = datetime.now(timezone.utc).isoformat()
            for product_id in cb_client.yield_products("USD"):
                data = cb_client.get_candles(start_time, end_time, product_id=product_id)
                for row in data:
                    writer.writerow([product_id] + row + [created_time])
                    
            pg_client.execute_sql("/app/sql/truncate_landing_candles.sql")
            pg_client.copy_from("/tmp/candles.csv", "landing.candles")
            os.remove("/tmp/candles.csv")
            pg_client.execute_sql("/app/sql/load_ods_candles.sql")
