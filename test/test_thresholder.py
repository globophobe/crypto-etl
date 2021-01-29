from copy import deepcopy

from cryptotick.aggregators.threshold.constants import NOTIONAL
from cryptotick.aggregators.threshold.lib import (
    aggregate_threshold,
    get_initial_threshold_cache,
)

from .utils import get_data_frame


def get_samples(trades, cache=None, thresh_attr=NOTIONAL, thresh_value=2):
    data_frame, _ = get_data_frame(trades)
    cache = cache or get_initial_threshold_cache(thresh_attr)
    return aggregate_threshold(data_frame, cache, thresh_attr, thresh_value)


def test_threshold():
    trades = [{"notional": 1, "ticks": [1, 1]}]
    samples, cache = get_samples(trades)
    assert len(samples) == 1


def test_threshold_with_cache():
    trades_1 = [{"notional": 1, "ticks": [1, 1, 1]}]
    trades_2 = [{"notional": 1, "ticks": [1]}]
    samples_1, c = get_samples(trades_1)
    cache_1 = deepcopy(c)
    assert "nextDay" in c
    samples_2, cache_2 = get_samples(trades_2, cache=c)
    assert samples_2[0]["open"] == cache_1["nextDay"]["open"]
    assert "nextDay" not in cache_2
