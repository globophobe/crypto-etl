from datetime import timezone

import numpy as np


def utc_timestamp(data_frame):
    # Because pyarrow.lib.ArrowInvalid: Casting from timestamp[ns]
    # to timestamp[us, tz=UTC] would lose data.
    data_frame.timestamp = data_frame.apply(
        lambda x: x.timestamp.tz_localize(timezone.utc), axis=1
    )
    return data_frame


def strip_nanoseconds(data_frame):
    # Bitmex data is accurate to the nanosecond.
    # However, data is typically only provided to the microsecond.
    data_frame["nanoseconds"] = data_frame.apply(
        lambda x: x.timestamp.nanosecond, axis=1
    )
    data_frame.timestamp = data_frame.apply(
        lambda x: x.timestamp.replace(nanosecond=0)
        if x.nanoseconds > 0
        else x.timestamp,
        axis=1,
    )
    return data_frame


def calculate_notional(data_frame, func):
    data_frame["notional"] = data_frame.apply(func, axis=1)
    return data_frame


def calculate_tick_rule(data_frame):
    data_frame["tickRule"] = data_frame.apply(
        lambda x: (1 if x.tickDirection in ("PlusTick", "ZeroPlusTick") else -1),
        axis=1,
    )
    return data_frame


def calculate_index(data_frame):
    symbols = data_frame.symbol.unique()
    data_frame["index"] = np.nan  # B/C pandas index
    for symbol in symbols:
        index = data_frame.index[data_frame.symbol == symbol]
        # 0-based index according to symbol
        data_frame.loc[index, "index"] = index.values - index.values[0]
    return data_frame


def set_columns(data_frame):
    data_frame = data_frame.rename(columns={"trdMatchID": "uid", "size": "volume"})
    return data_frame


def set_types(data_frame):
    return data_frame.astype(
        {
            "price": "float64",
            "volume": "float64",
            "notional": "float64",
            "index": "int64",
        }
    )


def row_to_json(row):
    return {
        "timestamp": row.timestamp.to_pydatetime(),
        "nanoseconds": row.nanoseconds,
        "price": row.price,
        "volume": row.volume,
        "notional": row.notional,
        "tickRule": row.tickRule,
        "index": row["index"],  # B/C pandas index
    }
