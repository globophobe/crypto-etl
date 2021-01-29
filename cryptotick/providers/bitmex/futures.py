from .base import BaseBitmex, BitmexFuturesDailyS3Mixin


class BitmexFuturesDailyPartition(BitmexFuturesDailyS3Mixin, BaseBitmex):
    def has_data(self, date):
        # No active symbols 2016-10-01 to 2016-10-25.
        return super().has_data(date)
