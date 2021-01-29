import datetime
import random

import pandas as pd
from cryptotick.aggregators.trades.lib import aggregate_trades

from .utils import get_data_frame, get_trade


def get_samples(trades):
    data_frame, has_multiple_symbols = get_data_frame(trades)
    return aggregate_trades(data_frame, has_multiple_symbols=has_multiple_symbols)


def test_equal_symbols_and_timestamps_and_ticks():
    trades = [{"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]}]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_symbols_and_timestamps_and_not_equal_ticks():
    trades = [{"symbol": "A", "is_equal_timestamp": True, "ticks": [1, -1]}]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_not_equal_symbols_and_equal_timestamps_and_ticks():
    trades = [
        {"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "B", "is_equal_timestamp": True, "ticks": [1, 1]},
    ]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_not_equal_symbols_and_timestamps_and_equal_ticks():
    trades = [
        {"symbol": "A", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "A", "is_equal_timestamp": False, "ticks": [-1]},
        {"symbol": "B", "is_equal_timestamp": True, "ticks": [1, 1]},
        {"symbol": "B", "is_equal_timestamp": False, "ticks": [-1]},
    ]
    samples = get_samples(trades)
    assert len(samples) == 4


def test_equal_ticks_and_equal_timestamp():
    trades = [{"ticks": [1, 1], "is_equal_timestamp": True}]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_ticks_and_not_equal_timestamp():
    trades = [{"ticks": [1, 1], "is_equal_timestamp": False}]
    samples = get_samples(trades)
    assert len(samples) == 2


def test_equal_ticks_and_equal_nanoseconds():
    trades = [
        {
            "ticks": [1, 1],
            "is_equal_timestamp": True,
            "nanoseconds": random.random() * 100,
        }
    ]
    samples = get_samples(trades)
    assert len(samples) == 1


def test_equal_ticks_and_not_equal_nanoseconds():
    trades = []
    nanoseconds = random.random() * 100
    for index, tick in enumerate([1, 1]):
        nanoseconds += index
        trade = {
            "ticks": [tick],
            "is_equal_timestamp": True,
            "nanoseconds": nanoseconds,
        }
        trades.append(trade)
    samples = get_samples(trades)
    assert len(samples) == 2


def test_slippage():
    now = datetime.datetime.now()
    trades = [
        get_trade(timestamp=now, price=1, notional=10, tick_rule=1),
        get_trade(timestamp=now, price=2, notional=10, tick_rule=1),
    ]
    data_frame = pd.DataFrame(trades)
    df = aggregate_trades(data_frame)
    assert df.loc[0].slippage == 10
