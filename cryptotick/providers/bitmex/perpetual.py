from .base import BaseBitmex, BitmexDailyS3Mixin

# ETH Slippage?!


class BitmexPerpetualDailyPartition(BitmexDailyS3Mixin, BaseBitmex):
    pass
