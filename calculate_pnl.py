import pandas as pd
import mplfinance as mpf
from datetime import datetime, timedelta
import glob
import matplotlib.pyplot as plt
import numpy as np
import math


def np_vwap(h, l, v):
    return np.cumsum(v * (h + l) / 2) / np.cumsum(v)


def buy_hold(ohlc_file, start_offset, stop_offset):
    daily_df = pd.read_csv(
        ohlc_file,
        parse_dates=True,
    )
    daily_df["Date"] = pd.to_datetime((daily_df["timestamp"] / 1000), unit="s")
    # daily_df["vwap"] = np_vwap(daily_df["volume"],daily_df["high"], daily_df["low"])
    daily_df["avg"] = (
        daily_df["open"] + daily_df["high"] + daily_df["low"] + daily_df["close"]
    ) / 4
    daily_df = daily_df.set_index("Date")

    # print(daily)
    start_offset_dt = datetime.fromtimestamp(
        daily_df.iloc[0]["timestamp"] / 1000
    ) + timedelta(minutes=start_offset)
    start_offset_ts = math.trunc(start_offset_dt.timestamp() * 1000)
    stop_offset_dt = start_offset_dt + timedelta(minutes=stop_offset)
    stop_offset_ts = math.trunc(stop_offset_dt.timestamp() * 1000)

    range_df = daily_df[
        (daily_df["timestamp"] >= start_offset_ts)
        & (daily_df["timestamp"] <= stop_offset_ts)
    ]

    avg_line = mpf.make_addplot(range_df["avg"])

    # mpf.plot(range_df,type='candle',mav=(3,6,9),volume=False,addplot=avg_line)

    buying_price = range_df.iloc[0]["avg"]
    selling_price = range_df.iloc[-1]["avg"]

    pnl = selling_price - buying_price
    pnl_pct = (pnl / buying_price) * 100

    ticker = ohlc_file.split("/")[-1].split(".")[0]

    return {
        "ticker": ticker,
        "start_time": start_offset_dt.isoformat(),
        "end_time": stop_offset_dt.isoformat(),
        "buying_price": buying_price,
        "selling_price": selling_price,
        "pnl": round(pnl, 4),
        "pnl_pct": round(pnl_pct, 4),
    }


def calculate_pnl():
    listings_file = glob.glob("./ohlc_data/*.csv")
    results = []
    for listing_file in listings_file:
        # TODO: Check number of rows in file, if < 2 skip file
        result = buy_hold(listing_file, 0, 180)
        results.append(result)

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(by="start_time", ascending=False)

    profitable_df = results_df[results_df["pnl_pct"] > 0]
    winning_pct = math.trunc((len(profitable_df) / len(results_df)) * 100)
    print("Win rate:", str(winning_pct) + "%")
