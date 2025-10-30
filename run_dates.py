#!/usr/bin/env python3
"""Convenience script for backfilling one or more trading dates."""

from __future__ import annotations

import argparse
from typing import Sequence

from db import init_db_connection
from main import backfill_for_dates


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments and gracefully ignore unknown flags.

    The script historically accepted only date arguments (without flags), but
    other commands in the repository expose options such as
    ``--analyze-regression``. When users run those commands together the
    additional flags were treated as dates, ultimately causing a
    ``DateParseError``. ``parse_known_args`` lets us separate the supported
    positional dates from any other options, keeping the script backwards
    compatible while preventing crashes.
    """

    parser = argparse.ArgumentParser(
        description="Backfill 1-minute bars for the supplied trading dates",
        allow_abbrev=False,
    )
    parser.add_argument(
        "dates",
        nargs="+",
        help="Trading dates to backfill (format: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help=(
            "Optional list of tickers to backfill. By default all tickers from "
            "tickers.csv plus SPY and VIX are processed."
        ),
    )

    args, unknown = parser.parse_known_args(argv)

    if unknown:
        print(f"⚠️  Ignoring unsupported arguments: {' '.join(unknown)}")

    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_arguments(argv)

    init_db_connection()
    backfill_for_dates(args.dates, tickers=args.tickers)
    print("\n✅ Done loading historic 1-minute bars!")


if __name__ == "__main__":
    main()
