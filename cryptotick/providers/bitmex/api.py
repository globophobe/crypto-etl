import datetime
import re
import time

import httpx
from yapic import json

from .constants import API_URL, BITMEX, MAX_API_RESULTS, MIN_DATE, MONTHS, XBT, XBTUSD


def parse_api_timestamp(timestamp):
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1]
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")


def get_active_futures(root_symbol=XBT, date_from=MIN_DATE, date_to=None, verbose=True):
    endpoint = "instrument/active"
    return get_futures(endpoint, root_symbol, date_from, date_to, verbose=verbose)


def get_expired_futures(
    root_symbol=XBT, date_from=MIN_DATE, date_to=None, verbose=True
):
    endpoint = "instrument"
    return get_futures(endpoint, root_symbol, date_from, date_to, verbose=verbose)


def get_futures(endpoint, root_symbol, date_from, date_to, verbose=True):
    filters = json.dumps({"rootSymbol": root_symbol})
    url = f"{API_URL}/{endpoint}?filter={filters}"
    results = iterate_api(
        url,
        date_from=date_from,
        date_to=date_to,
        end_time_key="timestamp",
        verbose=verbose,
    )
    instruments = []
    regex = re.compile(f"^{root_symbol}" + r"(\w)\d+$")
    for instrument in results:
        symbol = instrument["symbol"]
        match = regex.match(symbol)
        if match:
            is_future = match.group(1) in MONTHS
            if is_future:
                listing = parse_api_timestamp(instrument["listing"])
                expiry = parse_api_timestamp(instrument["expiry"])
                if expiry.date() >= date_from:
                    instruments.append(
                        {"symbol": symbol, "listing": listing, "expiry": expiry}
                    )
    return instruments


def get_funding(symbol=XBTUSD, date_from=None, date_to=None, verbose=False):
    url = f"{API_URL}/funding?symbol={symbol}"
    funding = iterate_api(
        url,
        date_from=date_from,
        date_to=date_to,
        end_time_key="timestamp",
        verbose=verbose,
    )
    funding = [
        {"timestamp": parse_api_timestamp(f["timestamp"]), "rate": f["fundingRate"]}
        for f in funding
    ]
    return funding


def iterate_api(url, date_from=None, date_to=None, end_time_key=None, verbose=True):
    if date_to:
        # B/C reversed.
        end_time = datetime.datetime.combine(date_to, datetime.datetime.min.time())
        end_time = end_time.isoformat()
    else:
        end_time = None
    results = []
    stop_execution = False
    while not stop_execution:
        response = get_api_response(url, end_time=end_time)
        if response:
            data = response.json()
            results += data
            if verbose:
                exchange = BITMEX.capitalize()
                if end_time:
                    print(f"{exchange}: {end_time}")
            if len(data) < MAX_API_RESULTS:
                stop_execution = True
            else:
                end_time = parse_api_timestamp(data[-1][end_time_key])
    return results


def get_api_response(url, end_time=None):
    url += "&count=500&reverse=true"
    if end_time:
        url += f"&endTime={end_time}"
    response = httpx.get(url)
    if response.status_code == 200:
        return response
    elif response.status_code == 429:
        retry = response.headers.get("Retry-After", 1)
        time.sleep(retry)
    else:
        exchange = BITMEX.capitalize()
        raise Exception(f"{exchange}: API {response.status_code}")
