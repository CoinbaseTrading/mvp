from datetime import timedelta

import os
import pandas as pd
import numpy as np

from cbt import slack
from cbt.pg import PostgresClient

if __name__ == "__main__":

    host = os.getenv("PG_HOST")
    database = os.getenv("PG_DATABASE")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
    slack_channel = os.getenv("SLACK_CHANNEL")
    lookback_days = int(os.getenv("LOOKBACK_DAYS", 5))

    pg_client = PostgresClient(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    conn = pg_client.get_connection()
    curs = conn.cursor()

    product_info = curs.execute(
        """
        SELECT
            TRIM(product_id),
            TO_TIMESTAMP(MIN(time)) AS min_timestamp,
            TO_TIMESTAMP(MAX(time)) AS max_timestamp,
            COUNT(*) AS samples
        FROM ods.candles
        GROUP BY product_id
        """
    )

    results = curs.fetchall()

    product_df = pd.DataFrame(results)
    product_df.columns = ["ticker", "min_ts", "max_ts", "samples"]

    # Calculate the time range using lookback and enforcing same final timestamp
    range_ts_max = np.min(product_df.max_ts)
    range_ts_min = range_ts_max - timedelta(days=lookback_days)

    price_info = curs.execute(
        """
        SELECT
            TRIM(product_id),
            time,
            close
        FROM ods.candles
        WHERE time BETWEEN {min_time} AND {max_time}
        ORDER BY product_id, time
        """.format(
            min_time=int(range_ts_min.timestamp()),
            max_time=int(range_ts_max.timestamp()),
        )
    )

    results = curs.fetchall()
    price_df = pd.DataFrame(results)
    price_df.columns = ["ticker", "time", "close"]
    price_df["close"] = price_df["close"].astype(np.float)
    price_df["log_close"] = np.log(price_df["close"])

    minutes_per_year = 525600

    ci_list = []
    for product in product_df.ticker:
        coin_df = price_df.loc[price_df.ticker == product].copy()

        # Snap the time and value at beginning and end of period
        first_price = coin_df.head(1).close.values[0]
        last_price = coin_df.tail(1).close.values[0]
        first_dt = pd.to_datetime(coin_df.head(1).time.values[0], unit="s")
        last_dt = pd.to_datetime(coin_df.tail(1).time.values[0], unit="s")

        # Inference on mean
        coin_df["delta_log_price"] = coin_df["log_close"].diff()
        coin_df["delta_log_price_lag1"] = coin_df["delta_log_price"].shift(1)

        coin_df = coin_df.iloc[1:]  # remove NaN caused by the diff
        auto_corr = np.corrcoef(
            coin_df["delta_log_price"].iloc[1:],
            coin_df["delta_log_price_lag1"].iloc[1:],
        )[0, 1]

        xbar = np.mean(coin_df["delta_log_price"])
        sd = np.std(coin_df["delta_log_price"])
        n = coin_df.shape[0]
        z = 1.645  # for 2-sided CI
        lower = xbar - z * sd / np.sqrt(n)
        upper = xbar + z * sd / np.sqrt(n)
        annualized_growth = np.exp(minutes_per_year * xbar)
        annualized_lower = np.exp(minutes_per_year * lower)
        annualized_upper = np.exp(minutes_per_year * upper)
        ci_list.append(
            {
                "product": product,
                "first_price": first_price,
                "last_price": last_price,
                "mean": xbar,
                "n_minutes": n,
                "sd": sd,
                "lower": lower,
                "upper": upper,
                "ann_growth": annualized_growth,
                "auto_corr": auto_corr,
            }
        )

    growth_df = pd.DataFrame(ci_list)
    growth_df = growth_df.sort_values(["lower"], ascending=False)

    best_coin = growth_df.head(1)
    message = """
        Study Timeframe: {start_ts}, {end_ts} on {minutes} minutes of data.
        If you were going to buy one coin, {product} is your safest bet
        with 2-sided 90% confidence log-difference CI [{lower_limit}, {upper_limit}]
        and estimated mean log-difference {mean}.
        The price started at {first_price} and ended at {last_price} over the
        time range.
    """.format(
        start_ts=str(range_ts_min),
        end_ts=str(range_ts_max),
        minutes=best_coin["n_minutes"].values[0],
        product=best_coin["product"].values[0],
        ann_growth=best_coin["ann_growth"].values[0],
        lower_limit=best_coin["lower"].values[0],
        upper_limit=best_coin["upper"].values[0],
        mean=best_coin["mean"].values[0],
        first_price=best_coin["first_price"].values[0],
        last_price=best_coin["last_price"].values[0],
    )

    if slack_bot_token and slack_channel:
        slack.post_to_channel(slack_bot_token, slack_channel, message)
    else:
        print(message)
