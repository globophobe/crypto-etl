import httpx
import pandas as pd

from ...cryptotick import CryptoTick, CryptoTickDailyS3Mixin
from ...s3downloader import calculate_notional
from .constants import BYBIT, URL
from .lib import calc_notional


class BaseBybit(CryptoTick):
    def __init__(
        self,
        symbol,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        super().__init__(
            BYBIT,
            symbol,
            period_from=period_from,
            period_to=period_to,
            aggregate=aggregate,
            verbose=verbose,
        )


class BybitDailyS3Mixin(CryptoTickDailyS3Mixin):
    def get_url(self, date):
        directory = f"{URL}{self.symbol}/"
        response = httpx.get(directory)
        if response.status_code == 200:
            return f"{URL}{self.symbol}/{self.symbol}{date.isoformat()}.csv.gz"
        else:
            print(f"{self.exchange.capitalize()} {self.symbol}: No data")

    def parse_dataframe(self, data_frame):
        # No false positives.
        # Source: https://pandas.pydata.org/pandas-docs/stable/user_guide/
        # indexing.html#returning-a-view-versus-a-copy
        pd.options.mode.chained_assignment = None
        # Bybit is reversed.
        data_frame = data_frame.iloc[::-1]
        data_frame["index"] = data_frame.index.values[::-1]
        data_frame["timestamp"] = pd.to_datetime(data_frame["timestamp"], unit="s")
        data_frame = super().parse_dataframe(data_frame)
        data_frame = calculate_notional(data_frame, calc_notional)
        return data_frame
