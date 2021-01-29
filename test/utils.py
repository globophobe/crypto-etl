import datetime
import random

import pandas as pd


def get_trade(
    symbol=None,
    timestamp=None,
    nanoseconds=None,
    price=None,
    slippage=0,
    notional=None,
    tick_rule=None,
):
    p = price or round(random.random() * 10, 2)
    n = notional or random.random() * 10
    volume = p * n
    data = {
        "timestamp": timestamp or datetime.datetime.now(),
        "nanoseconds": nanoseconds or 0,
        "price": p,
        "slippage": slippage,
        "volume": volume,
        "notional": n,
        "tickRule": tick_rule or random.choice((1, -1)),
    }
    if symbol:
        data["symbol"] = symbol
    return data


def get_trades(
    ticks, symbol=None, notional=None, is_equal_timestamp=False, nanoseconds=None
):
    """Generate random trades"""
    trade = get_trade(
        symbol=symbol, notional=notional, nanoseconds=nanoseconds, tick_rule=ticks[0]
    )
    trades = [trade]
    for tick in ticks[1:]:
        t = get_trade(
            symbol=symbol,
            timestamp=trade["timestamp"] if is_equal_timestamp else None,
            nanoseconds=nanoseconds or 0,
            price=trade["price"] + (round(random.random() * 0.1, 2)) * tick,
            notional=notional,
            tick_rule=tick,
        )
        trades.append(t)
    return trades


def get_data_frame(all_trades):
    symbols = [trade["symbol"] for trade in all_trades if "symbol" in trade]
    if len(symbols):
        # All or None
        assert len(all_trades) == len(symbols)
        first_symbol = symbols[0]
        has_multiple_symbols = all([first_symbol == symbol for symbol in symbols])
    else:
        has_multiple_symbols = False
    trades = []
    for trade in all_trades:
        ticks = trade.pop("ticks")
        trades += get_trades(ticks, **trade)
    data_frame = pd.DataFrame(trades)
    return data_frame, has_multiple_symbols
