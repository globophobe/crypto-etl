from ...utils import parse_period_from_to
from .spot import CoinbaseDailyPartition, CoinbaseHourlyPartition


def coinbase_spot(
    api_symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    aggregate: bool = False,
    verbose: bool = False,
):
    assert api_symbol
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        CoinbaseHourlyPartition(
            api_symbol=api_symbol,
            period_from=timestamp_from,
            period_to=timestamp_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        CoinbaseDailyPartition(
            api_symbol=api_symbol,
            period_from=date_from,
            period_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()
