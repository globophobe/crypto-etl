from ...bqloader import MULTIPLE_SYMBOL_AGGREGATE_SCHEMA, SINGLE_SYMBOL_AGGREGATE_SCHEMA
from ..base import BaseAggregator
from .lib import aggregate_trades


class BaseTradeAggregator(BaseAggregator):
    def __init__(self, source_table, **kwargs):
        destination_table = source_table + "_aggregated"
        super().__init__(source_table, destination_table, **kwargs)

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_AGGREGATE_SCHEMA
        else:
            return SINGLE_SYMBOL_AGGREGATE_SCHEMA

    def get_data_frame(self):
        # Query by partition.
        if self.has_multiple_symbols:
            fields = "timestamp, nanoseconds, symbol, price, volume, notional, tickRule"
            order_by = "symbol, timestamp, nanoseconds, index"
        else:
            fields = "timestamp, nanoseconds, price, volume, notional, tickRule"
            order_by = "timestamp, nanoseconds, index"
        sql = f"""
            SELECT {fields}
            FROM {self.source_table}
            WHERE {self.where_clause}
            ORDER BY {order_by};
        """
        bigquery_loader = self.get_bigquery_loader(
            self.source_table, self.partition_decorator
        )
        return bigquery_loader.read_table(sql, self.job_config)

    def process_data_frame(self, data_frame):
        df = aggregate_trades(
            data_frame, has_multiple_symbols=self.has_multiple_symbols
        )
        df["index"] = df.index
        return df[self.columns]
