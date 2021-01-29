# What?

This is the basis of a pipeline for cryptocurrency exchange tick data. 

# Why?

Historical tick data can be useful for analyzing financial markets, and for machine learning. As an example, refer to ["The Volume Clock: Insights into the High Frequency Paradigm"](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2034858).

It is said that 80% of the effort in ML is getting and cleaning your data. Maybe this can help.

# How?

When possible, data is downloaded from AWS S3 repositories. Otherwise, it is downloaded from the cryptocurrency exchange REST API.

Data is stored in Google BigQuery, because it is cost performant. For the price of a cup of coffee, years of historical tick data can be stored. Historical data state is stored in Firestore.

Cryptocurrency exchanges
----------------------------------

* [BitMEX](https://www.bitmex.com/) from the [S3](https://public.bitmex.com/) repository
* [Bybit](https://www.bybit.com/) from the [S3](https://public.bybit.com/) repository
* [Coinbase](https://www.coinbase.com/) REST API
* [FTX](https://ftx.com/) REST API

Public dataset
--------------

[The `cryptotick` dataset is public on BigQuery](https://console.cloud.google.com/bigquery?dataset=cryptotick&project=machine-learning-178613). If you have a GCP account, you can query the dataset. As part of the [free tier](https://cloud.google.com/free), a terabyte of queries is free each month.

Example data
------------

| uid |    timestamp (1)   | nanoseconds (2) | price  | volume |   notional   | tickRule (3) | index (4) |
|-----|--------------------|-----------------|--------|--------|--------------|--------------|-----------|
| ... | ...00:27:17.367156 | 0               | 456.98 | 2000   | 4.3765591... | -1           | 9         |
| ... | ...00:44:59.302471 | 0               | 457.1  | 1000   | 2.1877050... | 1            | 10        |
| ... | ...00:44:59.302471 | 0               | 457.11 | 2000   | 4.3753144... | 1            | 11        |

Note:

1. Timestamp
* [Partitioning data](https://cloud.google.com/bigquery/docs/partitioned-tables) is important when using BigQuery. `cryptotick` partitions data both daily and hourly by timestamp.

2. Nanoseconds
* BigQuery doesn't support nanoseconds. Then again, most cryptocurrency exchanges don't either. If present, nanoseconds will be saved to a new column by cryptotick.

3. Tick rule
* Plus ticks and zero plus ticks have tick rule 1, minus ticks and zero minus ticks have tick rule -1.

4. Index
* Trades may not be unique by timestamp and nanoseconds. Therefore, order is by timestamp, nanoseconds, and index.


Example aggregated data
-----------------------

Trades are aggregated by equal timestamp, nanoseconds, and tick rule. Referring to the previous table, trades 10 and 11 are aggregated into a single trade. Those trades have equal timestamps and, are both market buys.

| uid |    timestamp       | ... | price  | slippage (1) | volume |   notional   | tickRule | index |
|-----|--------------------|-----|--------|--------------|--------|--------------|----------|-------|
| ... | ...00:27:17.367156 | ... | 456.98 | 0            | 2000   | 4.3765591... | -1       |  ...  |
| ... |...00:44:59.302471  | ... | 457.11 | 0.043753     | 3000   | 6.5630195... | 1        |  ...  |

Note:

1. Slippage
* The difference between the ideal execution at the first price and the weighted average price by notional, of the fully executed trade. For example, a trader market buys 20 notional at $1. However, only 10 notional is offered at $1, the next offer for an additional 10 notional is at $2. In this case, ideal execution is $1 * 20 = $20. Actual execution is ($1 * 10) + ($2 * 10) = $30. Slippage would be $30 - 20 = $10.

Installation
------------

For convenience, `cryptotick` can be installed from PyPI:

```
pip install cryptotick
```

Environment
-----------

To use the scripts or deploy to GCP, rename `env.yaml.sample` to `env.yaml`, and add the required settings.
