from ...cryptotick import CryptoTickDailyMixin, CryptoTickHourlyMixin
from .base import BaseFTX


class FTXHourlyPartition(CryptoTickHourlyMixin, BaseFTX):
    pass


class FTXDailyPartition(CryptoTickDailyMixin, BaseFTX):
    pass
