from .base import BaseBybit, BybitDailyS3Mixin


class BybitPerpetualDailyPartition(BybitDailyS3Mixin, BaseBybit):
    pass
