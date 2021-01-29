import pandas as pd


def is_sample(last_row, row, has_multiple_symbols=False):
    if has_multiple_symbols:
        is_equal_symbol = last_row.symbol == row.symbol
    else:
        is_equal_symbol = True
    is_equal_timestamp = last_row.timestamp == row.timestamp
    is_equal_nanoseconds = last_row.nanoseconds == row.nanoseconds
    is_equal_tick = last_row.tickRule == row.tickRule
    return (
        not is_equal_symbol
        or not is_equal_timestamp
        or not is_equal_nanoseconds
        or not is_equal_tick
    )


def aggregate_trades(data_frame, has_multiple_symbols=False):
    idx = 0
    samples = []
    total_rows = len(data_frame) - 1
    # Were there two or more trades?
    if len(data_frame) > 1:
        for row in data_frame.itertuples():
            index = row.Index
            last_index = index - 1
            if index > 0:
                is_last_iteration = index == total_rows
                last_row = data_frame.loc[last_index]
                # Is this the last iteration?
                if is_last_iteration:
                    last_row = data_frame.loc[idx]
                    next_row = data_frame.loc[index]
                    # If equal, one sample.
                    if not is_sample(last_row, next_row, has_multiple_symbols):
                        # Aggregate from idx to end of data frame.
                        sample = data_frame.loc[idx:]
                        samples.append(aggregate_trade(sample, has_multiple_symbols))
                    # Otherwise, two samples.
                    else:
                        # Aggregate from idx to last_index
                        sample = data_frame.loc[idx:last_index]
                        samples.append(aggregate_trade(sample, has_multiple_symbols))
                        # Append last row.
                        sample = data_frame.loc[index:]
                        assert len(sample) == 1
                        samples.append(aggregate_trade(sample, has_multiple_symbols))
                # Is the last row equal to the current row?
                elif is_sample(last_row, row, has_multiple_symbols):
                    # Aggregate from idx to last_index
                    sample = data_frame.loc[idx:last_index]
                    aggregated_sample = aggregate_trade(sample, has_multiple_symbols)
                    samples.append(aggregated_sample)
                    idx = index
    # Only one trade in data_frame
    elif len(data_frame) == 1:
        aggregated_sample = aggregate_trade(data_frame)
        samples.append(aggregated_sample)
    # Instantiate dataframe
    data_frame = pd.DataFrame(samples)
    # Are there any samples?
    if len(data_frame):
        # Round slippage
        data_frame.slippage = data_frame.slippage.round(6)
    return data_frame


def aggregate_trade(data_frame, has_multiple_symbols=False):
    last_row = data_frame.iloc[-1]
    timestamp = last_row.timestamp
    notional = data_frame.notional.sum()
    if has_slippage(data_frame):
        slippage = calculate_slippage(data_frame, notional)
    else:
        slippage = 0
    data = {
        "date": timestamp.date(),
        "timestamp": timestamp,
        "nanoseconds": last_row.nanoseconds,
        "price": last_row.price,
        "slippage": slippage,
        "volume": data_frame.volume.sum(),
        "notional": notional,
        "ticks": len(data_frame),
        "tickRule": last_row.tickRule,
    }
    if has_multiple_symbols:
        data.update({"symbol": last_row.symbol})
    return data


def has_slippage(data_frame):
    if len(data_frame) > 1:
        prices = data_frame.price.to_numpy()
        first_price = prices[0]
        # Are all the prices equal?
        return not (first_price == prices).all()
    return False


def calculate_slippage(data_frame, notional):
    first_row = data_frame.iloc[0]
    expected = first_row.price * notional
    vwap = volume_weighted_average_price(data_frame)
    actual = vwap * notional
    return abs(expected - actual)


def volume_weighted_average_price(data_frame):
    vwap = data_frame.volume.cumsum() / data_frame.notional.cumsum()
    return vwap.iloc[-1]
