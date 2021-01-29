#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.aggregators import trade_aggregator
from cryptotick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(trade_aggregator)
