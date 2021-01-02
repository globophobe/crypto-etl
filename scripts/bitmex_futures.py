#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.bitmex import XBT, BitmexFuturesETL
from cryptotick.utils import set_environment, json_str_or_list


def bitmex_futures(
    root_symbol: str = XBT,
    date_from: str = None,
    date_to: str = None,
    aggregate: bool = False,
    post_aggregation: str = None,
    verbose: bool = False,
):
    set_environment()
    BitmexFuturesETL(
        root_symbol,
        date_from=date_from,
        date_to=date_to,
        aggregate=aggregate,
        post_aggregation=json_str_or_list(post_aggregation),
        verbose=verbose,
    ).main()


if __name__ == "__main__":
    typer.run(bitmex_futures)
