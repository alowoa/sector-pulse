"""
FRED macro data ingestion script.

Fetches selected FRED series and saves each as a clean CSV under data/fred/.

Default series (6-month window):
  - DGS10       : 10-Year Treasury Constant Maturity Rate (daily)
  - CPILFESL    : Core CPI (Less Food & Energy), monthly
  - BAMLH0A0HYM2: ICE BofA US High Yield OAS (daily)
  - FEDFUNDS    : Effective Federal Funds Rate (monthly)
  - MANEMP      : Manufacturing employment (monthly)

Usage:
  uv run python scripts/ingest_fred.py
  uv run python scripts/ingest_fred.py --months 12
  uv run python scripts/ingest_fred.py --start 2024-01-01 --end 2024-12-31
  uv run python scripts/ingest_fred.py --series DGS10 FEDFUNDS --months 3
"""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_SERIES: dict[str, str] = {
    "DGS10":        "10-Year Treasury Constant Maturity Rate",
    "CPILFESL":     "Core CPI (Less Food & Energy, SA)",
    "BAMLH0A0HYM2": "ICE BofA US High Yield Option-Adjusted Spread",
    "FEDFUNDS":     "Effective Federal Funds Rate",
    "MANEMP":       "All Employees: Manufacturing",
}

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "fred"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_series(fred: Fred, series_id: str, start: date, end: date) -> pd.DataFrame:
    """Fetch a single FRED series and return a tidy DataFrame."""
    raw = fred.get_series(series_id, observation_start=start, observation_end=end)
    df = (
        raw
        .rename("value")
        .rename_axis("date")
        .reset_index()
        .dropna(subset=["value"])
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["series_id"] = series_id
    return df[["date", "series_id", "value"]]


def save_csv(df: pd.DataFrame, series_id: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{series_id.lower()}.csv"
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest FRED macro series to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--months",
        type=int,
        default=6,
        metavar="N",
        help="Number of months back from today (default: 6).",
    )
    date_group.add_argument(
        "--start",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Explicit start date (requires --end).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="End date (default: today). Used with --start or --months.",
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="SERIES_ID",
        help="Override the list of series to fetch (e.g. DGS10 FEDFUNDS).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        metavar="DIR",
        help=f"Directory for CSV output (default: {OUTPUT_DIR}).",
    )
    return parser.parse_args()


def resolve_dates(args: argparse.Namespace) -> tuple[date, date]:
    end = args.end
    if args.start:
        return args.start, end
    # Approximate months → days (30 days/month)
    start = end - timedelta(days=args.months * 30)
    return start, end


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("ERROR: FRED_API_KEY is not set. Add it to your .env file.", file=sys.stderr)
        sys.exit(1)

    args = parse_args()
    start, end = resolve_dates(args)

    series_map = (
        {sid: DEFAULT_SERIES.get(sid, sid) for sid in args.series}
        if args.series
        else DEFAULT_SERIES
    )

    fred = Fred(api_key=api_key)

    print(f"Fetching {len(series_map)} series from {start} to {end}...")
    print(f"Output directory: {args.output_dir}\n")

    success, failure = [], []

    for series_id, description in series_map.items():
        try:
            df = fetch_series(fred, series_id, start, end)
            path = save_csv(df, series_id, args.output_dir)
            print(f"  \033[32m✓\033[0m  {series_id:<20} {len(df):>4} rows  →  {path.name}  ({description})")
            success.append(series_id)
        except Exception as exc:
            print(f"  \033[31m✗\033[0m  {series_id:<20} FAILED: {exc}")
            failure.append(series_id)

    print(f"\nDone — {len(success)} succeeded, {len(failure)} failed.")
    if failure:
        sys.exit(1)


if __name__ == "__main__":
    main()
