import time
from operator import eq, le

import httpx
import pandas as pd
import pendulum
from google.api_core.exceptions import ServiceUnavailable

from .bqloader import (
    SINGLE_SYMBOL_SCHEMA,
    BigQueryDaily,
    BigQueryHourly,
    get_schema_columns,
    get_table_id,
)
from .fscache import FirestoreCache, firestore_data, get_collection_name
from .s3downloader import (
    HistoricalDownloader,
    calculate_tick_rule,
    row_to_json,
    set_columns,
    set_types,
    strip_nanoseconds,
    utc_timestamp,
)
from .utils import parse_period_from_to


class CryptoTick:
    def __init__(
        self,
        exchange,
        symbol,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        self.exchange = exchange
        self.symbol = symbol
        self.period_from = period_from
        self.period_to = period_to
        self.aggregate = aggregate
        self.verbose = verbose

    @property
    def schema(self):
        return SINGLE_SYMBOL_SCHEMA

    @property
    def exchange_display(self):
        return self.exchange.capitalize()

    def get_suffix(self, sep="_"):
        return self.symbol

    @property
    def log_prefix(self):
        return f"{self.exchange_display} {self.symbol}"

    def get_partition_decorator(self, value):
        raise NotImplementedError

    @property
    def firestore_cache(self):
        suffix = self.get_suffix(sep="-")
        collection = get_collection_name(self.exchange, suffix=suffix)
        return FirestoreCache(collection)

    def get_document_name(self, partition):
        raise NotImplementedError

    def get_last_document_name(self, partition):
        raise NotImplementedError

    def get_document(self, partition):
        document = self.get_document_name(partition)
        return self.firestore_cache.get(document)

    def get_last_document(self, partition):
        document = self.get_last_document_name(partition)
        return self.firestore_cache.get(document)

    def has_data(self, partition):
        document = self.get_document_name(partition)
        if self.firestore_cache.has_data(document):
            if self.verbose:
                print(f"{self.log_prefix}: {document} OK")
            return True

    def get_firebase_data(self, df):
        if len(df):
            open_price = df.head(1).iloc[0]
            low_price = df.loc[df["price"].idxmin()]
            high_price = df.loc[df["price"].idxmax()]
            close_price = df.tail(1).iloc[0]
            buy_side = df[df["tickRule"] == 1]
            volume = float(df["volume"].sum())
            buy_volume = float(buy_side["volume"].sum())
            notional = float(df["notional"].sum())
            buy_notional = float(buy_side["notional"].sum())
            ticks = len(df)
            buy_ticks = len(buy_side)
            return {
                "open": firestore_data(row_to_json(open_price)),
                "low": firestore_data(row_to_json(low_price)),
                "high": firestore_data(row_to_json(high_price)),
                "close": firestore_data(row_to_json(close_price)),
                "volume": volume,
                "buyVolume": buy_volume,
                "notional": notional,
                "buyNotional": buy_notional,
                "ticks": ticks,
                "buyTicks": buy_ticks,
            }
        return {}

    def set_firebase(self, data, attr="firestore_cache", is_complete=False, retry=5):
        document = self.get_document_name(self.partition)
        # If dict, assume correct
        if isinstance(data, pd.DataFrame):
            data = self.get_firebase_data(data)
        data["ok"] = is_complete
        # Retry n times
        r = retry - 1
        try:
            getattr(self, attr).set(document, data)
        except ServiceUnavailable as exception:
            if r == 0:
                raise exception
            else:
                time.sleep(1)
                self.set_firebase(data, attr=attr, is_complete=is_complete, retry=r)
        else:
            print(f"{self.log_prefix}: {document} OK")

    def get_bigquery_loader(self, table_id, partition_value):
        raise NotImplementedError

    def iter_partition(self):
        raise NotImplementedError

    def main(self):
        raise NotImplementedError


class CryptoTickREST(CryptoTick):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pagination_id = None
        self.maybe_complete = False
        self.trades = []

    @property
    def url(self):
        raise NotImplementedError

    @property
    def max_requests_per_second(self):
        raise NotImplementedError

    @property
    def can_paginate(self):
        return True

    def get_pagination_id(self, data):
        raise NotImplementedError

    def has_data(self, partition):
        data = self.get_document(partition)
        if data:
            # Fake current partition, as probably not complete
            if self.partition == partition:
                ok = True
            # Other partitions probably complete
            else:
                ok = data.get("ok", False)
            if ok:
                # Pagination
                self.pagination_id = self.get_pagination_id(data)
                if self.verbose:
                    document = self.get_document_name(partition)
                    print(f"{self.log_prefix}: {document} OK")
            return ok

    def main(self):
        for partition in self.iter_partition():
            self.trades = self.get_valid_trades(self.trades, operator=le)
            if not self.has_data(partition):
                if self.maybe_trades(partition) and self.can_paginate:
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
                elif len(self.trades) and self.can_paginate:
                    self.update()
                # Maybe iteration complete
                elif self.maybe_complete and self.verbose:
                    print(f"{self.log_prefix}: Maybe complete")
                    break

    def maybe_trades(self, partition):
        # Do trades exceed partition boundaries?
        if len(self.trades):
            last_timestamp = self.trades[-1]["timestamp"]
            return self.get_partition(last_timestamp) >= partition
        return True

    def get_valid_trades(self, trades, operator=eq):
        return [
            trade
            for trade in trades
            if operator(
                self.get_partition_decorator(trade["timestamp"]),
                self.partition_decorator,
            )
        ]

    def get_data(self):
        response = self.get_response()
        if response:
            if response.status_code == 200:
                return self.parse_response(response)
            else:
                raise Exception(f"{response.status_code}: {response.content}")
        else:
            raise Exception(f"{self.log_prefix}: No response")

    def get_response(self, retry=5):
        e = None
        # Retry n times.
        for i in range(retry):
            try:
                return httpx.get(self.url)
            except Exception as exception:
                e = exception
                time.sleep(i + 1)
        raise e

    def parse_response(self, response):
        raise NotImplementedError

    def parse_data(self, data):
        return [
            {
                "uid": self.get_uid(trade),
                "timestamp": self.get_timestamp(trade),
                "nanoseconds": self.get_nanoseconds(trade),
                "price": self.get_price(trade),
                "volume": self.get_volume(trade),
                "notional": self.get_notional(trade),
                "tickRule": self.get_tick_rule(trade),
                "index": self.get_index(trade),
            }
            for trade in data
        ]

    def get_uid(self, trade):
        raise NotImplementedError

    def get_timestamp(self, trade):
        raise NotImplementedError

    def get_price(self, trade):
        raise NotImplementedError

    def get_volume(self, trade):
        raise NotImplementedError

    def get_notional(self, trade):
        raise NotImplementedError

    def get_tick_rule(self, trade):
        raise NotImplementedError

    def get_index(self, trade):
        raise NotImplementedError

    def update_trades(self, trades):
        if len(trades):
            start = trades[-1]
            partition_complete = (
                self.partition_decorator
                != self.get_partition_decorator(start["timestamp"])
            )
            self.trades += trades
            # Verbose
            if self.verbose:
                timestamp = start["timestamp"].replace(tzinfo=None).isoformat()
                index = start["index"]
                if not start["timestamp"].microsecond:
                    timestamp += ".000000"
                print(f"{self.log_prefix}: {timestamp} {index}")
            if partition_complete or self.maybe_complete:
                self.update()
                return True
        else:
            return True

    def update(self):
        data = self.get_valid_trades(self.trades)
        # Are there any trades?
        if len(data):
            self.write(data)
        # No trades
        else:
            data = self.get_document(self.partition)
            self.set_firebase({}, is_complete=True)

    def write(self, trades):
        start = trades[-1]
        stop = trades[0]
        assert self.get_partition_decorator(
            start["timestamp"]
        ) == self.get_partition_decorator(stop["timestamp"])
        # Dataframe
        columns = get_schema_columns(self.schema)
        data_frame = pd.DataFrame(trades, columns=columns)
        data_frame = set_types(data_frame)
        self.assert_data_frame(data_frame, trades)
        data = self.get_last_document(self.partition)
        is_complete = data is not None
        # Assert last trade
        if is_complete:
            self.assert_is_complete(trades)
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=suffix)
        bigquery_loader = self.get_bigquery_loader(table_id, self.partition_decorator)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        data_frame = data_frame.iloc[::-1]  # Reverse data frame
        self.set_firebase(data_frame, is_complete=is_complete)

    def assert_data_frame(self, data_frame, trades):
        # Are trades unique?
        assert len(data_frame) == len(data_frame.uid.unique())

    def assert_is_complete(self, trades):
        pass


class CryptoTickHourlyMixin:
    def get_suffix(self, sep="_"):
        return f"{self.symbol}{sep}hot"

    def get_document_name(self, timestamp):
        return timestamp.strftime("%Y-%m-%dT%H")  # Date, plus hour

    def get_last_document_name(self, timestamp):
        last_partition = self.get_last_partition(timestamp)
        return self.get_document_name(last_partition)

    def get_partition(self, timestamp):
        return timestamp

    def get_last_partition(self, timestamp):
        timestamp += pd.Timedelta("1h")
        return timestamp

    def get_partition_decorator(self, timestamp):
        return timestamp.strftime("%Y%m%d%H")  # Partition by hour

    def get_bigquery_loader(self, table_id, partition_decorator):
        return BigQueryHourly(table_id, partition_decorator)

    def iter_partition(self):
        period = pendulum.period(self.period_to, self.period_from)  # Reverse order
        for partition in period.range("hours"):
            self.partition = partition
            self.partition_decorator = self.get_partition_decorator(partition)
            yield partition


class CryptoTickDailyMixin:
    def get_document_name(self, date):
        return date.isoformat()  # Date

    def get_last_document_name(self, date):
        last_partition = self.get_last_partition(date)
        return self.get_document_name(last_partition)

    def get_last_partition(self, date):
        date += pd.Timedelta("1d")
        return date

    def has_data(self, partition):
        ok = super().has_data(partition)
        # Hourly has pagination_id
        if not ok and hasattr(self, "pagination_id"):
            if not self.pagination_id:
                timestamp_from, _, _, date_to = parse_period_from_to()
                # Maybe hourly
                if partition == date_to:
                    document_name = timestamp_from.strftime("%Y-%m-%dT%H")
                    collection = f"{self.firestore_cache.collection}-hot"
                    data = FirestoreCache(collection).get(document_name)
                    if data:
                        self.pagination_id = self.get_pagination_id(data)
        return ok

    def get_partition(self, timestamp):
        return timestamp.date()

    def get_partition_decorator(self, date):
        return date.strftime("%Y%m%d")  # Partition by date

    def get_bigquery_loader(self, table_id, partition_decorator):
        return BigQueryDaily(table_id, partition_decorator)

    def iter_partition(self):
        period = pendulum.period(self.period_to, self.period_from)  # Reverse order
        for partition in period.range("days"):
            self.partition = partition
            self.partition_decorator = self.get_partition_decorator(partition)
            yield partition


class CryptoTickDailyS3Mixin(CryptoTickDailyMixin):
    def get_url(self, partition):
        raise NotImplementedError

    def main(self):
        for partition in self.iter_partition():
            self.partition_decorator = self.get_partition_decorator(partition)
            document = self.get_document_name(partition)
            if not self.has_data(partition):
                url = self.get_url(partition)
                if self.verbose:
                    print(f"{self.log_prefix}: downloading {document}")
                data_frame = HistoricalDownloader(url).main()
                if data_frame is not None:
                    df = self.filter_dataframe(data_frame)
                    if len(df):
                        self.process_dataframe(df)
                    else:
                        if self.verbose:
                            print(f"{self.log_prefix}: Maybe complete")
                        break
                else:
                    if self.verbose:
                        print(f"{self.log_prefix}: Maybe complete")
                    break

    def filter_dataframe(self, data_frame):
        return data_frame

    def process_dataframe(self, data_frame):
        data_frame = self.parse_dataframe(data_frame)
        if len(data_frame):
            self.write(data_frame)
        else:
            print(f"{self.log_prefix}: No data")

    def parse_dataframe(self, data_frame):
        # Transforms
        data_frame = utc_timestamp(data_frame)
        data_frame = strip_nanoseconds(data_frame)
        data_frame = set_columns(data_frame)
        data_frame = calculate_tick_rule(data_frame)
        return data_frame

    def write(self, data_frame):
        # Types
        data_frame = set_types(data_frame)
        # Columns
        columns = get_schema_columns(self.schema)
        data_frame = data_frame[columns]
        # BigQuery
        suffix = self.get_suffix(sep="_")
        table_id = get_table_id(self.exchange, suffix=suffix)
        bigquery_loader = self.get_bigquery_loader(table_id, self.partition_decorator)
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, is_complete=True)
