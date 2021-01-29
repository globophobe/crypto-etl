import datetime

import pandas as pd

from ...bqloader import MULTIPLE_SYMBOL_SCHEMA
from ...cryptotick import CryptoTick, CryptoTickDailyS3Mixin
from ...s3downloader import calculate_index, calculate_notional
from .api import get_active_futures, get_expired_futures
from .constants import BITMEX, URL
from .lib import calc_notional


class BaseBitmex(CryptoTick):
    def __init__(
        self,
        symbol,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        super().__init__(
            BITMEX,
            symbol,
            period_from=period_from,
            period_to=period_to,
            aggregate=aggregate,
            verbose=verbose,
        )


class BitmexDailyS3Mixin(CryptoTickDailyS3Mixin):
    def get_url(self, date):
        date_string = date.strftime("%Y%m%d")
        return f"{URL}{date_string}.csv.gz"

    def filter_dataframe(self, data_frame):
        return data_frame[data_frame.symbol == self.symbol]

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Reset index.
        data_frame = calculate_index(data_frame)
        # Timestamp
        data_frame["timestamp"] = pd.to_datetime(
            data_frame["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f"
        )
        data_frame = super().parse_dataframe(data_frame)
        # Notional after other transforms.
        data_frame = calculate_notional(data_frame, calc_notional)
        return data_frame


class BitmexFuturesDailyS3Mixin(BitmexDailyS3Mixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbols = self.get_symbols()

    def get_symbols(self):
        active_futures = get_active_futures(
            self.symbol,
            date_from=self.period_from,
            date_to=self.period_to,
            verbose=self.verbose,
        )
        expired_futures = get_expired_futures(
            self.symbol,
            date_from=self.period_from,
            date_to=self.period_to,
            verbose=self.verbose,
        )
        return active_futures + expired_futures

    @property
    def schema(self):
        return MULTIPLE_SYMBOL_SCHEMA

    def get_suffix(self, sep="-"):
        return f"{self.symbol}USD{sep}futures"

    @property
    def log_prefix(self):
        suffix = self.get_suffix(" ")
        return f"{self.exchange_display} {suffix}"

    @property
    def active_symbols(self):
        return [
            s
            for s in self.symbols
            if s["listing"].date() <= self.partition <= s["expiry"].date()
        ]

    def has_symbols(self, data):
        return all([data.get(s["symbol"], None) for s in self.active_symbols])

    def get_symbol_data(self, symbol):
        return [s for s in self.symbols if s["symbol"] == symbol][0]

    def has_data(self, date):
        """Firestore cache with keys for each symbol, all symbols have data."""
        document = date.isoformat()
        if not self.active_symbols:
            print(f"{self.log_prefix}: No data")
            return True
        else:
            data = self.firestore_cache.get(document)
            if data:
                ok = data.get("ok", False)
                if ok and self.has_symbols(data):
                    print(f"{self.log_prefix}: {document} OK")
                    return True

    def get_firebase_data(self, data_frame):
        data = {}
        for s in self.active_symbols:
            symbol = s["symbol"]
            # API data
            d = self.get_symbol_data(symbol)
            # Dataframe
            df = data_frame[data_frame["symbol"] == symbol]
            # Maybe symbol
            if len(df):
                data[symbol] = super().get_firebase_data(df)
                data[symbol]["listing"] = d["listing"].replace(
                    tzinfo=datetime.timezone.utc
                )
                data[symbol]["expiry"] = d["expiry"].replace(
                    tzinfo=datetime.timezone.utc
                )
                # for key in ("listing", "expiry"):
                #     value = data[symbol][key]
                #     if not hasattr(value, "_nanosecond"):
                #         setattr(value, "_nanosecond", 0)
            else:
                df[symbol] = {}
        return data

    def filter_dataframe(self, data_frame):
        query = " | ".join([f'symbol == "{s["symbol"]}"' for s in self.active_symbols])
        return data_frame.query(query)
