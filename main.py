from ciso8601 import parse_datetime

from cryptotick.aggregators import TradeAggregator
from cryptotick.providers.bitmex import (
    BitmexFuturesETL,
    BitmexFuturesETLTrigger,
    BitmexPerpetualETL,
    BitmexPerpetualETLTrigger,
)
from cryptotick.providers.bybit import BybitPerpetualETL, BybitPerpetualETLTrigger
from cryptotick.providers.coinbase import (
    CoinbaseSpotETL,
    CoinbaseSpotETLAIPlatformTrigger,
)
from cryptotick.providers.ftx import FTXMOVEETL, FTXPerpetualETL
from cryptotick.utils import base64_decode_event, get_delta, is_gte_2_days_ago


def bitmex(event, context):
    data = base64_decode_event(event)
    date = data.get("date", get_delta(days=-1))
    aggregate = data.get("aggregate", True)
    root_symbol = data.get("root_symbol", None)
    symbols = [s for s in data.get("symbols", "").split(" ") if s]
    assert not (
        root_symbol and symbols
    ), "Only 1 of root_symbols or symbols should be provided"
    if is_gte_2_days_ago(date):
        if root_symbol:
            BitmexMultiSymbolREST(
                root_symbol, date_from=date, date_to=date, aggregate=aggregate
            ).main()
        else:
            BitmexSingleSymbolREST(
                root_symbol, date_from=date, date_to=date, aggregate=aggregate
            ).main()
    else:
        if root_symbol:
            BitmexMultiSymbolS3(
                root_symbol, date_from=date, date_to=date, aggregate=aggregate
            ).main()
        elif symbols:
            BitmexSingleSymbolS3(
                symbols, date_from=date, date_to=date, aggregate=aggregate
            ).main()


def bybit_trigger(event, context):
    data = base64_decode_event(event)
    date = data.get("date", get_delta(days=-1).isoformat())
    symbols = [s for s in data.get("symbols", "").split(" ") if s]
    aggregate = data.get("aggregate", True)
    for symbol in symbols:
        BybitPerpetualETLTrigger(
            symbol, date_from=date, date_to=date, aggregate=aggregate
        ).main()


def bybit_perpetual(event, context):
    data = base64_decode_event(event)
    symbol = data.get("symbol", None)
    date = data.get("date", get_delta(days=-1).isoformat())
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    if symbol:
        BybitPerpetualETL(
            symbol, date_from=date, date_to=date, aggregate=aggregate, verbose=verbose
        ).main()


def ftx_move(event, context):
    data = base64_decode_event(event)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    FTXMOVEETL(
        date_from=date_from, date_to=date_to, aggregate=aggregate, verbose=verbose
    ).main()


def ftx_perpetual(event, context):
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    if api_symbol:
        FTXPerpetualETL(
            api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()


def coinbase_spot(event, context):
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    if api_symbol:
        CoinbaseSpotETL(
            api_symbol=api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()


def coinbase_spot_ai_platform_trigger(event, context):
    """
    Some Coinbase symbols, such as BTCUSD may run longer than 540 seconds.
    GCP AI platform training jobs, are essentially background jobs that
    run in Docker containers.  Although not the cheapest option, it is sufficient.
    """
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    if api_symbol:
        CoinbaseSpotETLAIPlatformTrigger(
            api_symbol=api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()


def trade_aggregator(event, context):
    data = base64_decode_event(event)
    table_id = data.get("table_id", None)
    date = data.get("date", get_delta(days=-1).isoformat())
    has_multiple_symbols = data.get("has_multiple_symbols", False)
    verbose = data.get("verbose", False)
    if table_id:
        TradeAggregator(
            table_id,
            date_from=date,
            date_to=date,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
