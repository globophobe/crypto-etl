import datetime

from yapic import json

from ...cryptotick import CryptoTickREST
from .constants import FTX, MAX_REQUESTS_PER_SECOND, MAX_RESULTS, URL


class BaseFTX(CryptoTickREST):
    def __init__(
        self,
        api_symbol,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        super().__init__(
            FTX,
            api_symbol.replace("-", ""),
            period_from=period_from,
            period_to=period_to,
            aggregate=aggregate,
            verbose=verbose,
        )
        self.api_symbol = api_symbol
        self.uids = []
        self.last_timestamp = None

    @property
    def url(self):
        url = f"{URL}/markets/{self.api_symbol}/trades?limit={MAX_RESULTS}"
        if self.pagination_id:
            url += f"&end_time={self.pagination_id}"
        return url

    @property
    def max_requests_per_second(self):
        return MAX_REQUESTS_PER_SECOND

    @property
    def exchange_display(self):
        return self.exchange.upper()

    def get_pagination_id(self, data):
        if data:
            if "open" in data:
                return data["open"]["timestamp"].timestamp()
            last_partition = self.get_last_partition(self.partition)
            if isinstance(last_partition, datetime.datetime):
                return last_partition.replace(
                    minute=0, second=0, microsecond=0
                ).timestamp()
            elif isinstance(last_partition, datetime.date):
                return datetime.datetime.combine(
                    last_partition, datetime.datetime.min.time()
                ).timestamp()

    def parse_response(self, response):
        data = json.loads(response.content)
        if data["success"]:
            trades = self.parse_data(data["result"])
            if len(trades):
                return self.update_trades(trades)

    def get_uid(self, trade):
        return str(trade["id"])

    def get_timestamp(self, trade):
        return trade["time"]

    def get_nanoseconds(self, trade):
        return 0

    def get_price(self, trade):
        return float(trade["price"])

    def get_volume(self, trade):
        return float(trade["price"]) * float(trade["size"])

    def get_notional(self, trade):
        return float(trade["size"])

    def get_tick_rule(self, trade):
        return 1 if trade["side"] == "buy" else -1

    def get_index(self, trade):
        return int(trade["id"])

    def update_trades(self, trades):
        # Are there duplicates?
        t = [trade for trade in trades if trade["uid"] not in self.uids]
        uids = [trade["uid"] for trade in trades]
        if len(t):
            last_timestamp = t[-1]["timestamp"]
            # Next pagination_id
            self.pagination_id = last_timestamp.replace(
                tzinfo=datetime.timezone.utc
            ).timestamp()
            if self.last_timestamp:
                # Is next second?
                if last_timestamp < self.last_timestamp:
                    # No duplicates
                    self.uids = uids
                    self.last_timestamp = last_timestamp.replace(
                        second=0, microsecond=0
                    ) + datetime.timedelta(minutes=-1)
                else:
                    # Maybe duplicates
                    self.uids += uids
            else:
                # First iteration
                self.uids = uids
                self.last_timestamp = last_timestamp.replace(
                    second=0, microsecond=0
                ) + datetime.timedelta(minutes=-1)
        # Were there more than MAX_RESULTS with same timestamp?
        elif len(trades) == MAX_RESULTS:
            pagination_id = self.pagination_id - 1e-6
            self.pagination_id = round(pagination_id, 6)
            return False  # Do not stop iteration
        # Maybe iteration complete
        if not len(t):
            self.update()
            self.maybe_complete = True
            return self.maybe_complete
        return super().update_trades(t)

    def assert_data_frame(self, data_frame, trades):
        super().assert_data_frame(data_frame, trades)
        # Assert incrementing ids
        diff = data_frame["index"].diff().dropna()
        assert all([value < 0 for value in diff.values])
