import pandas as pd


def get_timestamp_from_to(partition):
    timestamp_from = partition.replace(minute=0, second=0, microsecond=0)
    timestamp_to = timestamp_from + pd.Timedelta("1h")
    return timestamp_from, timestamp_to
