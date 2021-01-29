from google.cloud import bigquery

from ..bqloader import BigQueryDaily
from ..cryptotick import CryptoTick, CryptoTickDailyMixin
from ..fscache import FirestoreCache
from ..utils import parse_period_from_to


def updater(
    source_table: str = None,
    period_from: str = None,
    period_to: str = None,
    verbose: bool = False,
):
    assert source_table
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if date_from and date_to:
        Updater(
            source_table,
            period_from=date_from,
            period_to=date_to,
            verbose=verbose,
        ).main()


class Updater(CryptoTickDailyMixin, CryptoTick):
    def __init__(
        self,
        source_table,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        self.source_table = source_table
        self.period_from = period_from
        self.period_to = period_to
        self.verbose = verbose

    def get_source(self, sep="_"):
        _, table_name = self.source_table.split(".")
        return table_name.replace("_", sep)

    @property
    def log_prefix(self):
        return self.get_source(sep=" ")

    @property
    def firestore_cache(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    def main(self):
        for partition in self.iter_partition():
            data = self.get_document(partition)
            if data and "candles" in data:
                data_frame = self.get_data_frame()
                self.set_firebase(data_frame, is_complete=data["ok"])
        print(f"{self.log_prefix}: Maybe complete")

    def get_data_frame(self):
        # Query by partition.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", self.partition)
            ]
        )
        sql = f"""
          SELECT timestamp, nanoseconds, price, volume, notional, tickRule, index
          FROM {self.source_table}
          WHERE date(timestamp) = @date
          ORDER BY timestamp, nanoseconds, index;
        """
        partition_decorator = self.get_partition_decorator(self.partition)
        return BigQueryDaily(self.source_table, partition_decorator).read_table(
            sql, job_config
        )
