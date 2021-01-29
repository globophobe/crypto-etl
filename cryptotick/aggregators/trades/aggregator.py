from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseTradeAggregator


class HourlyTradeAggregator(HourlyAggregatorMixin, BaseTradeAggregator):
    pass


class DailyTradeAggregator(DailyAggregatorMixin, BaseTradeAggregator):
    pass
