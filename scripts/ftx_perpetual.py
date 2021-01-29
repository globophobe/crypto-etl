#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.providers.ftx import ftx_perpetual
from cryptotick.utils import set_environment


if __name__ == "__main__":
    set_environment()
    typer.run(ftx_perpetual)
