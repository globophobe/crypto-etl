import datetime
import re
import time

from yapic import json

from ...bqloader import MULTIPLE_SYMBOL_SCHEMA, get_table_id
from ...cryptotick import FuturesETL
from ...utils import date_range, get_delta, publish
from .api import get_active_futures, get_expired_futures
from .base import BaseFTXETL
from .constants import BTC, BTCMOVE, FTX, MAX_REQUESTS_PER_SECOND, MAX_RESULTS, MIN_DATE


class FTXMOVEETL(BaseFTXETL, FuturesETL):
    def __init__(
        self,
        date_from=None,
        date_to=None,
        schema=MULTIPLE_SYMBOL_SCHEMA,
        aggregate=False,
        verbose=False,
    ):
        self.exchange = FTX
        self.initialize_dates(MIN_DATE, date_from, date_to)
        self.root_symbol = BTC
        self.symbols = self.get_symbols(BTC)
        self.schema = schema
        self.verbose = verbose

        self.max_requests_per_second = MAX_REQUESTS_PER_SECOND
        self.pagination_id = None

    def get_suffix(self, sep="-"):
        return BTCMOVE.replace("-", "")

    def get_pagination_id(self, data=None):
        if data:
            for key in data:
                if key != "ok":
                    if "candles" in data[key]:
                        for candle in data[key]["candles"]:
                            if candle:
                                date = candle["open"]["timestamp"].date()
                                yesterday = get_delta(date, days=-1)
                                return self.get_timestamp(yesterday)

    def get_symbols(self, root_symbol):
        active_futures = get_active_futures(root_symbol, verbose=False)
        expired_futures = get_expired_futures(
            root_symbol,
            verbose=False,
        )
        regex = re.compile(r"^.+(Q\d)$")
        futures = active_futures + expired_futures
        move_futures = [f for f in futures if f["listing"]]
        for future in move_futures:
            delta = future["expiry"] - future["listing"]
            if delta.days <= 7:
                future["symbol"] = f"MOVE{delta.days}D"
            else:
                match = regex.match(future["api_symbol"])
                if match:
                    m = match.group(1)
                    future["symbol"] = f"MOVE{m}"
                else:
                    raise NotImplementedError
            # MOVE expires next day open, move to previous close.
            future["expiry"] -= datetime.timedelta(microseconds=1)
        return move_futures

    def has_data(self, date):
        """Firestore cache with keys for each symbol, all symbols have data."""
        if not self.active_symbols:
            print(f"{self.log_prefix}: No data")
            return True
        else:
            return super().has_data(date)

    def main(self):
        for date in date_range(self.date_from, self.date_to, reverse=True):
            self.date = date
            self.trades = []
            if not self.has_data(date):
                for symbol in self.active_symbols:
                    # Reset pagination_id
                    self.pagination_id = self.get_pagination_id()
                    self.symbol = symbol["symbol"]
                    self.api_symbol = symbol["api_symbol"]
                    stop_execution = False
                    while not stop_execution:
                        start_time = time.time()
                        for i in range(self.max_requests_per_second):
                            stop_execution = self.get_data()
                            if stop_execution:
                                break
                        if not stop_execution:
                            elapsed = time.time() - start_time
                            if elapsed < 1:
                                diff = 1 - elapsed
                                time.sleep(diff)
            if len(self.trades):
                self.write(self.trades)

        print(
            f"{self.log_prefix}: {self.date_from.isoformat()} to "
            f"{self.date_to.isoformat()} OK"
        )

    def parse_response(self, response):
        data = json.loads(response.content)
        if data["success"]:
            all_trades = self.parse_data(data["result"])
            # No results.
            if not len(all_trades):
                return True
            else:
                end_time = all_trades[-1]["timestamp"]
                self.pagination_id = end_time.timestamp()
                date = end_time.date()
                # Verbose
                if self.verbose:
                    index = all_trades[-1]["index"]
                    timestamp = end_time.isoformat()
                    if not end_time.microsecond:
                        timestamp += ".000000"
                    print(f"{self.log_prefix}: {timestamp} {index}")
                # Maybe duplicates
                ids = [trade["index"] for trade in self.trades]
                trades = [trade for trade in all_trades if trade["index"] not in ids]
                t = [t for t in trades if t["date"] == self.date]
                self.trades += t
                # No more trades on date.
                if self.date != date:
                    return True
                # No more results.
                elif len(all_trades) < 200:
                    return True
                # Maybe more than max results with same timestamp.
                elif len(all_trades) == MAX_RESULTS and not len(t):
                    pagination_id = self.pagination_id - 1e-6
                    self.pagination_id = round(pagination_id, 6)

    def parse_data(self, data):
        trades = []
        for d in data:
            timestamp = self.parse_timestamp(d)
            date = timestamp.date()
            trade = {
                "date": date,
                "symbol": self.get_symbol(),
                "timestamp": timestamp,
                "nanoseconds": 0,  # No nanoseconds.
                "price": self.get_price(d),
                "volume": self.get_volume(d),
                "notional": self.get_notional(d),
                "tickRule": self.get_tick_rule(d),
                "index": self.get_index(d),
            }
            trades.append(trade)
        return trades

    def get_symbol(self):
        return [
            s["symbol"]
            for s in self.active_symbols
            if s["api_symbol"] == self.api_symbol
        ][0]

    def assert_data_frame(self, data_frame, trades):
        pass

    def assert_is_complete(self, data, trades):
        if data:
            for key in data:
                if key != "ok":
                    if "candles" in data[key]:
                        for candle in data[key]["candles"]:
                            if candle:
                                assert candle["open"]["index"] > trades[0]["index"]
