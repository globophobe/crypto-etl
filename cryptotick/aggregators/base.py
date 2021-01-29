import datetime

from firebase_admin import firestore
from google.cloud import bigquery

from ..bqloader import get_schema_columns
from ..cryptotick import CryptoTick, CryptoTickDailyMixin, CryptoTickHourlyMixin
from ..fscache import FirestoreCache
from ..utils import get_delta
from .lib import get_timestamp_from_to


class BaseAggregator(CryptoTick):
    def __init__(
        self,
        source_table,
        destination_table,
        symbol=None,
        period_from=None,
        period_to=None,
        require_cache=False,
        has_multiple_symbols=False,
        verbose=False,
    ):
        self.source_table = source_table
        self.destination_table = destination_table
        self.symbol = symbol
        self.require_cache = require_cache
        self.has_multiple_symbols = has_multiple_symbols
        self.period_from = self.get_period_from(period_from)
        self.period_to = self.get_period_to(period_to)
        self.verbose = verbose

    def get_source(self, sep="_"):
        _, table_name = self.source_table.split(".")
        return table_name.replace("_", sep)

    def get_destination(self, sep="_"):
        _, table_name = self.destination_table.split(".")
        return table_name.replace("_", sep)

    @property
    def log_prefix(self):
        name = self.get_destination(sep=" ")
        return name[0].capitalize() + name[1:]  # Cap 1st char

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def columns(self):
        return get_schema_columns(self.schema)

    @property
    def firestore_source(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    @property
    def firestore_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

    def get_period(self, timestamp):
        raise NotImplementedError

    def get_period_from(self, period_from):
        data = self.firestore_source.get_one(order_by="open.timestamp")
        timestamps = self.get_timestamps_from_data(data, "open")
        if len(timestamps):
            timestamp = min(timestamps)
            p = self.get_period(timestamp)
            if p > period_from:
                return p
            return period_from

    def get_period_to(self, period_to):
        data = self.firestore_source.get_one(
            order_by="close.timestamp", direction=firestore.Query.DESCENDING
        )
        timestamps = self.get_timestamps_from_data(data, "close")
        if len(timestamps):
            timestamp = max(timestamps)
            p = self.get_period(timestamp)
            if p < period_to:
                return p
            return period_to

    def get_timestamps_from_data(self, data, attr):
        timestamps = []
        if data:
            if self.has_multiple_symbols:
                for key, symbol in data.items():
                    if isinstance(symbol, dict):
                        timestamp = symbol[attr]["timestamp"]
                        timestamps.append(timestamp)
                        break
            else:
                timestamps.append(data[attr]["timestamp"])
        return timestamps

    def get_initial_cache(self, data_frame):
        raise NotImplementedError

    def get_cache(self, data_frame):
        document = get_delta(self.date, days=-1).isoformat()
        data = self.firestore_destination.get(document)
        # Is cache required, and no data?
        if self.require_cache and not data:
            # Is date greater than min date?
            if self.date > self.date_from:
                date = self.date.isoformat()
                assert data, f"Cache does not exist, {date}"
        if not data:
            data_frame, data = self.get_initial_cache(data_frame)
        return data_frame, data

    def main(self):
        if self.period_from and self.period_to:
            for partition in self.iter_partition():
                self.partition_decorator = self.get_partition_decorator(partition)
                document = self.get_document_name(partition)
                if self.firestore_source.has_data(document):
                    if not self.firestore_destination.has_data(document):
                        data_frame = self.get_data_frame()
                        # Are there any trades?
                        if len(data_frame):
                            df = self.process_data_frame(data_frame)
                            self.write(df)
                        # No trades
                        else:
                            self.set_firebase(
                                {}, attr="firestore_destination", is_complete=True
                            )
                    elif self.verbose:
                        print(f"{self.log_prefix}: {document} OK")
                else:
                    print(f"{self.log_prefix}: {document} No data")
        else:
            print(f"{self.log_prefix}: No data")

    def get_data_frame(self):
        raise NotImplementedError

    @property
    def job_config(self):
        raise NotImplementedError

    @property
    def where_clause(self):
        raise NotImplementedError

    def aggregate(self, data_frame, cache):
        raise NotImplementedError

    def write(self, data_frame):
        # BigQuery
        bigquery_loader = self.get_bigquery_loader(
            self.destination_table, self.partition_decorator
        )
        bigquery_loader.write_table(self.schema, data_frame)
        # Firebase
        self.set_firebase(data_frame, attr="firestore_destination", is_complete=True)


class HourlyAggregatorMixin(CryptoTickHourlyMixin):
    def get_period(self, timestamp):
        return timestamp.replace(tzinfo=datetime.timezone.utc)

    @property
    def job_config(self):
        timestamp_from, timestamp_to = get_timestamp_from_to(self.partition)
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "timestamp_from", "TIMESTAMP", timestamp_from
                ),
                bigquery.ScalarQueryParameter(
                    "timestamp_to", "TIMESTAMP", timestamp_to
                ),
            ]
        )

    @property
    def where_clause(self):
        return "timestamp >= @timestamp_from and timestamp < @timestamp_to"


class DailyAggregatorMixin(CryptoTickDailyMixin):
    def get_period(self, timestamp):
        return timestamp.date()

    @property
    def job_config(self):
        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", self.partition)
            ]
        )

    @property
    def where_clause(self):
        return "date(timestamp) = @date"
