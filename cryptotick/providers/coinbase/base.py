from yapic import json

from ...cryptotick import CryptoTickREST
from .constants import COINBASE, MAX_REQUESTS_PER_SECOND, URL


class BaseCoinbase(CryptoTickREST):
    def __init__(
        self,
        api_symbol,
        period_from=None,
        period_to=None,
        aggregate=False,
        verbose=False,
    ):
        super().__init__(
            COINBASE,
            api_symbol.replace("-", ""),
            period_from=period_from,
            period_to=period_to,
            aggregate=aggregate,
            verbose=verbose,
        )
        self.api_symbol = api_symbol

    @property
    def url(self):
        url = f"{URL}/products/{self.api_symbol}/trades"
        if self.pagination_id:
            return f"{url}?after={self.pagination_id}"
        return url

    @property
    def max_requests_per_second(self):
        return MAX_REQUESTS_PER_SECOND

    @property
    def can_paginate(self):
        return self.pagination_id is None or int(self.pagination_id) > 1

    def get_pagination_id(self, data):
        if data and "open" in data:
            pagination_id = int(data["open"]["index"])
        else:
            last = self.firestore_cache.get_one(order_by="open.index")
            if "open" in last:
                pagination_id = int(last["open"]["index"])
        # Maybe iteration complete
        if pagination_id == 1:
            self.maybe_complete = True
        return pagination_id

    def parse_response(self, response):
        """
        Pagination details: https://docs.pro.coinbase.com/#pagination
        """
        # Coinbase says cursor pagination can be unintuitive at first.
        # After gets data older than cb-after pagination id.
        data = json.loads(response.content)
        trades = self.parse_data(data)
        # Update pagination_id
        pagination_id = response.headers.get("cb-after", None)
        if pagination_id:
            pagination_id = int(pagination_id)
            if self.pagination_id:
                assert self.pagination_id > pagination_id
            if pagination_id == 1:
                self.maybe_complete = True
            self.pagination_id = pagination_id
        else:
            self.maybe_complete = True
        return self.update_trades(trades)

    def get_uid(self, trade):
        return str(trade["trade_id"])

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
        # Buy side indicates a down-tick because the maker was a buy order and
        # their order was removed. Conversely, sell side indicates an up-tick.
        return 1 if trade["side"] == "sell" else -1

    def get_index(self, trade):
        return int(trade["trade_id"])

    def assert_is_complete(self, trades):
        assert self.firestore_cache.get_one(
            where=["open.index", "==", trades[0]["index"] + 1]
        )
