from ...utils import parse_period_from_to
from .aggregator import DailyTradeAggregator, HourlyTradeAggregator


def trade_aggregator(
    source_table: str = None,
    period_from: str = None,
    period_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    assert source_table
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        HourlyTradeAggregator(
            f"{source_table}_hot",
            period_from=timestamp_from,
            period_to=timestamp_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        DailyTradeAggregator(
            source_table,
            period_from=date_from,
            period_to=date_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
