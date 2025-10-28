"""Utility queries for analyzing QQQ holdings minute bars.

This module exposes helper functions to compare the intraday behaviour of the
QQQ ETF against the aggregated behaviour of its holdings stored in MongoDB.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd

from db import init_db_connection
from models import MinuteBar


@dataclass
class ComparisonResult:
    """Container for per-minute comparison data and daily totals."""

    per_minute: pd.DataFrame
    totals: pd.DataFrame


def _load_qqq_holdings(
    top_n: Optional[int] = None, *, sort_by_allocation: bool = False
) -> List[str]:
    """Return the list of tickers that are flagged as QQQ holdings."""

    df = pd.read_csv("tickers.csv")
    holding_flags = pd.to_numeric(df["QQQ_holding"], errors="coerce").fillna(0).astype(int)
    holdings_df = df.loc[holding_flags == 1, ["ticker", "allocation_percentage"]].copy()

    if holdings_df.empty:
        raise ValueError("No QQQ holdings found in tickers.csv")

    if sort_by_allocation:
        allocation = (
            holdings_df["allocation_percentage"]
            .astype(str)
            .str.rstrip("%")
        )
        holdings_df["_allocation"] = pd.to_numeric(allocation, errors="coerce").fillna(0.0)
        holdings_df = holdings_df.sort_values("_allocation", ascending=False)

    tickers = holdings_df["ticker"]
    if top_n is not None:
        tickers = tickers.head(top_n)

    return tickers.tolist()


def _query_minute_bars(tickers: Iterable[str], date: str) -> pd.DataFrame:
    """Fetch minute bar data for the supplied tickers and date."""

    init_db_connection()
    queryset = MinuteBar.objects(ticker__in=list(tickers), date=date)
    records = [
        {
            "ticker": doc.ticker,
            "minute_index": doc.minute_index,
            "timestamp": doc.timestamp,
            "open": doc.open,
            "high": doc.high,
            "low": doc.low,
            "close": doc.close,
            "volume": doc.volume,
        }
        for doc in queryset
    ]
    columns = ["ticker", "minute_index", "timestamp", "open", "high", "low", "close", "volume"]
    return pd.DataFrame(records, columns=columns)


def minute_top10_holdings_to_qqq_prediction(date: str, top_n: int = 10) -> pd.DataFrame:
    """
    Build a DataFrame of features from the top N holdings' 1-minute price changes
    with the target being the next-minute price change of QQQ.

    For each minute t:
      - For each of the top N holdings by allocation, compute ΔClose_t = Close_t − Open_t.
      - Use these Δs at minute t as predictor features.
      - Use the ΔPrice of QQQ at minute t+1 as the target variable.

    Returns a DataFrame with one row per minute_index containing columns
    holding_<ticker>_chg and target_qqq_change.
    """

    if top_n <= 0:
        raise ValueError("top_n must be a positive integer")

    holdings = _load_qqq_holdings(top_n=top_n, sort_by_allocation=True)
    holdings_df = _query_minute_bars(holdings, date)
    qqq_df = _query_minute_bars(["QQQ"], date)

    if holdings_df.empty:
        raise ValueError(f"No minute bar data found for top holdings on {date}")
    if qqq_df.empty:
        raise ValueError(f"No minute bar data found for QQQ on {date}")

    holdings_df["price_change"] = holdings_df["close"] - holdings_df["open"]
    pivot = holdings_df.pivot(index="minute_index", columns="ticker", values="price_change")
    pivot = pivot.rename(columns=lambda t: f"holding_{t}_chg")

    qqq_df["qqq_change"] = qqq_df["close"] - qqq_df["open"]
    qqq_by_minute = qqq_df.set_index("minute_index")["qqq_change"]

    df = pivot.copy()
    df["target_qqq_change"] = qqq_by_minute.shift(-1)
    df = df.dropna(subset=["target_qqq_change"])

    return df


def list_available_dates() -> List[str]:
    """Return sorted trading dates with both QQQ and holdings data available."""

    holdings = _load_qqq_holdings()
    init_db_connection()
    qqq_dates = set(MinuteBar.objects(ticker="QQQ").distinct("date"))
    holdings_dates = set(MinuteBar.objects(ticker__in=list(holdings)).distinct("date"))
    available = sorted(qqq_dates & holdings_dates)
    return available


def _build_totals_dataframe(**totals: float) -> pd.DataFrame:
    """Return a single-row DataFrame from keyword totals."""

    return pd.DataFrame([totals])


def minute_volume_comparison(date: str) -> ComparisonResult:
    """Compare aggregated 1-minute volumes for QQQ holdings vs. the QQQ ETF."""

    holdings = _load_qqq_holdings()
    holdings_df = _query_minute_bars(holdings, date)
    qqq_df = _query_minute_bars(["QQQ"], date)

    if holdings_df.empty:
        raise ValueError(f"No minute bar data found for QQQ holdings on {date}")
    if qqq_df.empty:
        raise ValueError(f"No minute bar data found for QQQ on {date}")

    holdings_volume = (
        holdings_df.groupby("minute_index")["volume"].sum().rename("holdings_volume")
    )
    qqq_volume = qqq_df.set_index("minute_index")["volume"].rename("qqq_volume")

    comparison = (
        pd.concat([holdings_volume, qqq_volume], axis=1)
        .fillna(0)
        .reset_index()
        .sort_values("minute_index")
    )
    comparison["volume_difference"] = comparison["holdings_volume"] - comparison["qqq_volume"]

    totals = _build_totals_dataframe(
        holdings_total_volume=int(holdings_volume.sum()),
        qqq_total_volume=int(qqq_volume.sum()),
        total_volume_difference=int(holdings_volume.sum() - qqq_volume.sum()),
    )

    return ComparisonResult(per_minute=comparison, totals=totals)


def minute_price_change_comparison(date: str) -> ComparisonResult:
    """Compare summed minute price changes for holdings vs. the QQQ ETF."""

    holdings = _load_qqq_holdings()
    holdings_df = _query_minute_bars(holdings, date)
    qqq_df = _query_minute_bars(["QQQ"], date)

    if holdings_df.empty:
        raise ValueError(f"No minute bar data found for QQQ holdings on {date}")
    if qqq_df.empty:
        raise ValueError(f"No minute bar data found for QQQ on {date}")

    holdings_df["price_change"] = holdings_df["close"] - holdings_df["open"]
    qqq_df["price_change"] = qqq_df["close"] - qqq_df["open"]

    holdings_change = (
        holdings_df.groupby("minute_index")["price_change"].sum().rename("holdings_price_change")
    )
    qqq_change = qqq_df.set_index("minute_index")["price_change"].rename("qqq_price_change")

    comparison = (
        pd.concat([holdings_change, qqq_change], axis=1)
        .fillna(0)
        .reset_index()
        .sort_values("minute_index")
    )
    comparison["price_change_difference"] = (
        comparison["holdings_price_change"] - comparison["qqq_price_change"]
    )

    totals = _build_totals_dataframe(
        holdings_total_price_change=float(holdings_change.sum()),
        qqq_total_price_change=float(qqq_change.sum()),
        total_price_change_difference=float(holdings_change.sum() - qqq_change.sum()),
    )

    return ComparisonResult(per_minute=comparison, totals=totals)


def _format_result(result: ComparisonResult, heading: str) -> str:
    """Return a pretty string representation for console usage."""

    parts = [heading, "Per-minute comparison:", result.per_minute.to_string(index=False), "", "Totals:", result.totals.to_string(index=False)]
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare QQQ ETF against aggregated holdings data")
    parser.add_argument("date", nargs="?", help="Trading day to query (YYYY-MM-DD)")
    parser.add_argument(
        "--list-dates",
        action="store_true",
        help="List trading dates that can be supplied to the positional date argument",
    )
    parser.add_argument(
        "--include-prediction",
        action="store_true",
        help=(
            "Display the top holdings minute change feature set alongside QQQ's next-minute change"
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top QQQ holdings (by allocation) to include in the prediction dataset",
    )
    args = parser.parse_args()

    if args.list_dates:
        dates = list_available_dates()
        if dates:
            print("Available trading dates:")
            for trading_date in dates:
                print(f" - {trading_date}")
        else:
            print("No trading dates found in the database for QQQ and its holdings.")
        if args.date is None:
            return

    if args.date is None:
        parser.error("the following arguments are required: date")

    if args.top_n <= 0:
        parser.error("--top-n must be a positive integer")

    volume_result = minute_volume_comparison(args.date)
    price_change_result = minute_price_change_comparison(args.date)

    print(_format_result(volume_result, "Volume comparison"))
    print("\n" + "-" * 80 + "\n")
    print(_format_result(price_change_result, "Price change comparison"))

    if args.include_prediction:
        prediction_df = minute_top10_holdings_to_qqq_prediction(args.date, top_n=args.top_n)
        print("\n" + "-" * 80 + "\n")
        print(
            "Top holdings minute-change features vs. next-minute QQQ change:\n"
            + prediction_df.to_string()
        )


if __name__ == "__main__":
    main()
