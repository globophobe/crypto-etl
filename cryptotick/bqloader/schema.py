from google.cloud import bigquery

SINGLE_SYMBOL_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED"),  # For verification
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]

MULTIPLE_SYMBOL_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
] + SINGLE_SYMBOL_SCHEMA[1:]


SINGLE_SYMBOL_AGGREGATE_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


MULTIPLE_SYMBOL_AGGREGATE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
] + SINGLE_SYMBOL_AGGREGATE_SCHEMA


SINGLE_SYMBOL_BAR_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED"),  # For join
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("open", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("high", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("low", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("close", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buySlippage", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("ticks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
    bigquery.SchemaField(
        "topN",
        "RECORD",
        mode="REPEATED",
        fields=(
            bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("nanoseconds", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("slippage", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("volume", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
        ),
    ),
]

MULTIPLE_SYMBOL_BAR_SCHEMA = [
    bigquery.SchemaField("uid", "STRING", "REQUIRED"),  # For join
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
] + SINGLE_SYMBOL_BAR_SCHEMA[2:]
