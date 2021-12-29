import csv
import ccxt
import json
from datetime import datetime, timedelta


class FetchHistory:
    def __init__(self, exchange_id) -> None:
        self.exchange = getattr(ccxt, exchange_id)(
            {
                "enableRateLimit": True,
                # Add Proxy incase of downloading huge data
                # "proxies": {
                #     "http": "http://pubproxy.com/api/proxy?limit=1&format=txt&type=socks5",
                # },
            }
        )
        self.exchange.load_markets()
        self.max_retries = 3

    def retry_fetch_ohlcv(self, symbol, timeframe, since, limit):
        num_retries = 0
        try:
            num_retries += 1
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            return ohlcv
        except Exception:
            if num_retries > self.max_retries:
                raise  # Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')

    def scrape_ohlcv(self, symbol, timeframe, since, until, limit):
        timeframe_duration_in_seconds = self.exchange.parse_timeframe(timeframe)
        timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
        timedelta = limit * timeframe_duration_in_ms
        all_ohlcv = []
        fetch_since = since
        fetch_until = until

        while fetch_since < fetch_until:
            ohlcv = self.retry_fetch_ohlcv(
                symbol,
                timeframe,
                fetch_since,
                limit,
            )
            if ohlcv != None:

                fetch_since = (
                    (ohlcv[-1][0] + 1) if len(ohlcv) else (fetch_since + timedelta)
                )
                all_ohlcv = all_ohlcv + ohlcv
                if len(all_ohlcv):
                    if len(all_ohlcv) == 0:
                        print("Candles not present in ", symbol)

                    print(
                        len(all_ohlcv),
                        "candles in total from",
                        self.exchange.iso8601(all_ohlcv[0][0]),
                        "to",
                        self.exchange.iso8601(all_ohlcv[-1][0]),
                    )
                else:
                    print(
                        len(all_ohlcv),
                        "candles in total from",
                        self.exchange.iso8601(fetch_since),
                    )
            else:
                print("ohlc is None")

        return self.exchange.filter_by_since_limit(all_ohlcv, since, None, key=0)

    def write_to_csv(self, filename, data):
        with open(filename, mode="w") as output_file:
            csv_writer = csv.writer(
                output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csv_writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])

            csv_writer.writerows(data)

    def scrape_candles_to_csv(self, filename, symbol, timeframe, since, until, limit):
        # convert since from string to milliseconds integer if needed
        if isinstance(since, str):
            since = self.exchange.parse8601(since)

        if isinstance(until, str):
            until = self.exchange.parse8601(until)

        is_present = False
        try:
            ticker_state = self.exchange.fetch_ticker(symbol)
            is_present = True

        except:
            is_present = False
            print("Error Fetching ticker", symbol)

        if is_present:
            # fetch all candles
            ohlcv = self.scrape_ohlcv(symbol, timeframe, since, until, limit)
            # save them to csv file
            self.write_to_csv(filename, ohlcv)
            print(
                "Saved",
                len(ohlcv),
                "candles from",
                self.exchange.iso8601(ohlcv[0][0]),
                "to",
                self.exchange.iso8601(ohlcv[-1][0]),
                "to",
                filename,
            )


LISTINGS_FILE_DIR = "./listings_spider/listings.json"

# TODO: Filter tickers, e.g blacklist leveraged tokens
if __name__ == "__main__":
    f = open(LISTINGS_FILE_DIR)
    listings = json.load(f)
    print(len(listings))
    currency = "USDT"
    currency_prefix = "-"
    handler = FetchHistory("kucoin")

    for listing in listings:
        if listing["publish_ts"] > 0:
            try:

                output_file_name = listing["ticker"] + ".csv"
                output_file = "./ohlc_data/" + output_file_name
                ticker_name = listing["ticker"].upper()
                dataname = ticker_name + currency_prefix + currency

                start_time = datetime.fromtimestamp(listing["trading_ts"]).isoformat()
                end_time = datetime.fromtimestamp(listing["trading_ts"]) + timedelta(
                    minutes=15
                )
                end_time = end_time.isoformat()

                print(start_time)
                print(end_time)
                print("****")

                # print(end_time.isoformat())
                handler.scrape_candles_to_csv(<
                    output_file,
                    dataname,
                    "1m",
                    start_time,
                    end_time,
                    180,  # Fetch 180 1 minute rows
                )

            except Exception as err:
                print(err)
