"""
SPDR sector ETF OHLCV ingestion script.

Downloads daily OHLCV data for the 11 SPDR sector ETFs over a rolling window
(default: 6 months) via yfinance.  Saves one CSV per ticker and one
consolidated file under data/yfinance/.

Usage:
  uv run python scripts/ingest_ohlcv.py
  uv run python scripts/ingest_ohlcv.py --months 12
  uv run python scripts/ingest_ohlcv.py --start 2024-01-01 --end 2024-12-31
  uv run python scripts/ingest_ohlcv.py --tickers XLK XLF --output-dir data/yfinance
"""

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPDR_TICKERS: list[str] = [
    "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLC", "XLU", "XLY", "XLP", "XLRE", "XLB",
]

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "yfinance"

RETRY_DELAYS = [10, 30, 60]
MAX_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(output_dir: Path) -> logging.Logger:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "ingest_ohlcv.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        ],
    )
    return logging.getLogger("ingest_ohlcv")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_with_retry(
    tickers: list[str],
    start: date,
    end: date,
    logger: logging.Logger,
    attempts: int = MAX_ATTEMPTS,
    delays: list[int] = RETRY_DELAYS,
) -> pd.DataFrame:
    """Bulk-download OHLCV via yf.download() with exponential retry."""
    last_exc: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            logger.info(
                f"Attempt {attempt}/{attempts} — bulk download ({len(tickers)} tickers)..."
            )
            raw = yf.download(
                tickers=tickers,
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=True,
                group_by="ticker",
                progress=False,
                threads=True,
            )
            if raw is None or raw.empty:
                raise ValueError("yf.download() returned an empty DataFrame.")
            logger.info(f"Download succeeded — raw shape: {raw.shape}")
            return raw
        except Exception as exc:
            last_exc = exc
            if attempt < attempts:
                wait = delays[attempt - 1]
                logger.warning(f"[{type(exc).__name__}] {exc} — retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Failed after {attempts} attempts: {exc}")

    raise RuntimeError(
        f"Could not download data after {attempts} attempts."
    ) from last_exc


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_ticker_df(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Extract a single-ticker OHLCV DataFrame from the bulk download result.
    Returns columns: date, open, high, low, close, volume, ticker.
    """
    if isinstance(raw.columns, pd.MultiIndex):
        try:
            df = raw[ticker].copy()
        except KeyError:
            raise ValueError(f"{ticker}: not found in raw DataFrame.")
    else:
        df = raw.copy()

    df.columns = [c.lower() for c in df.columns]

    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{ticker}: missing OHLCV columns: {missing}")

    df = df.dropna(how="all").reset_index()

    date_col = next((c for c in df.columns if c.lower() in ("date", "datetime")), None)
    if date_col is None:
        raise ValueError(f"{ticker}: date column not found after reset_index.")

    df = df.rename(columns={date_col: "date"})
    df["date"]   = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["open"]   = df["open"].astype(float).round(4)
    df["high"]   = df["high"].astype(float).round(4)
    df["low"]    = df["low"].astype(float).round(4)
    df["close"]  = df["close"].astype(float).round(4)
    df["volume"] = df["volume"].fillna(0).astype(np.int64)
    df["ticker"] = ticker

    return (
        df[["date", "open", "high", "low", "close", "volume", "ticker"]]
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def quality_check(df: pd.DataFrame, ticker: str, logger: logging.Logger) -> dict:
    """Run QA checks on a ticker DataFrame; return a report dict."""
    report: dict = {
        "rows": len(df),
        "null_count": {},
        "ohlc_violations": 0,
        "gaps_detected": [],
        "status": "OK",
    }

    # QA 1: null values
    numeric_cols = ["open", "high", "low", "close", "volume"]
    null_counts = df[numeric_cols].isnull().sum().to_dict()
    report["null_count"] = {k: int(v) for k, v in null_counts.items()}
    if sum(null_counts.values()) > 0:
        logger.warning(f"[QA-NULL] {ticker} — null values: {null_counts}")
        report["status"] = "WARNING"

    # QA 2: OHLC integrity
    violations = ~(
        (df["high"] >= df[["open", "close"]].max(axis=1))
        & (df["low"] <= df[["open", "close"]].min(axis=1))
        & (df["open"] > 0)
        & (df["close"] > 0)
    )
    n_violations = int(violations.sum())
    report["ohlc_violations"] = n_violations
    if n_violations > 0:
        bad_dates = df.loc[violations, "date"].tolist()
        logger.warning(
            f"[QA-OHLC] {ticker} — {n_violations} violation(s) at: "
            f"{bad_dates[:10]}{'...' if len(bad_dates) > 10 else ''}"
        )
        report["status"] = "WARNING"

    # QA 3: gaps > 1 business day (tolerate weekends=3d, long weekends=4d)
    dates = pd.to_datetime(df["date"]).sort_values().reset_index(drop=True)
    gaps = []
    if len(dates) > 1:
        for i, delta in dates.diff().dropna().items():
            if delta.days > 4:
                gap = {
                    "from": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                    "to": dates.iloc[i].strftime("%Y-%m-%d"),
                    "days": delta.days,
                }
                gaps.append(gap)
                logger.warning(
                    f"[QA-GAP] {ticker} — {delta.days}-day gap: "
                    f"{gap['from']} → {gap['to']}"
                )

    report["gaps_detected"] = gaps
    if len(gaps) > 3:
        raise ValueError(
            f"{ticker}: {len(gaps)} data gaps detected (threshold=3). Check source."
        )
    if gaps and report["status"] == "OK":
        report["status"] = "WARNING"

    return report


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_individual(df: pd.DataFrame, ticker: str, output_dir: Path) -> Path:
    path = output_dir / f"{ticker}_ohlcv_6m.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    return path


def save_consolidated(frames: list[pd.DataFrame], output_dir: Path) -> tuple[pd.DataFrame, Path]:
    consolidated = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    path = output_dir / "ALL_SPDR_ohlcv_6m.csv"
    consolidated.to_csv(path, index=False, encoding="utf-8")
    return consolidated, path


def save_qa_report(report: dict, output_dir: Path) -> Path:
    path = output_dir / "qa_report.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest SPDR sector ETF OHLCV data to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--months",
        type=int,
        default=6,
        metavar="N",
        help="Rolling window in months back from today (default: 6).",
    )
    date_group.add_argument(
        "--start",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Explicit start date (requires --end or uses today).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Override the ticker list (default: all 11 SPDR ETFs).",
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
    return end - timedelta(days=args.months * 30), end


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    start, end = resolve_dates(args)
    tickers = args.tickers or SPDR_TICKERS
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    run_date = date.today().isoformat()

    logger.info(f"Period     : {start} → {end}")
    logger.info(f"Tickers    : {tickers}")
    logger.info(f"Output dir : {output_dir}")
    logger.info(f"yfinance   : {yf.__version__}")

    qa_report: dict = {
        "run_date": run_date,
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "tickers_processed": [],
        "tickers_failed": [],
        "per_ticker": {},
        "consolidated_rows": 0,
        "yfinance_version": yf.__version__,
    }

    raw = download_with_retry(tickers, start, end, logger)

    frames: list[pd.DataFrame] = []

    for ticker in tickers:
        logger.info(f"--- {ticker} ---")
        try:
            df = extract_ticker_df(raw, ticker)
            ticker_qa = quality_check(df, ticker, logger)
            qa_report["per_ticker"][ticker] = ticker_qa

            csv_path = save_individual(df, ticker, output_dir)
            logger.info(f"{ticker}: {ticker_qa['rows']} rows → {csv_path}")

            frames.append(df)
            qa_report["tickers_processed"].append(ticker)

            status_icon = "\033[32m✓\033[0m" if ticker_qa["status"] == "OK" else "\033[33m⚠\033[0m"
            print(
                f"  {status_icon}  {ticker:<6} — {ticker_qa['rows']:>4} rows"
                f"  {df['date'].min()} → {df['date'].max()}"
            )

        except ValueError as exc:
            logger.error(f"[QA ERROR] {ticker}: {exc}")
            qa_report["tickers_failed"].append(ticker)
            qa_report["per_ticker"][ticker] = {"status": "ERROR", "error": str(exc)}
            print(f"  \033[31m✗\033[0m  {ticker:<6} — ERROR: {exc}")

        except Exception as exc:
            logger.error(f"[ERROR] {ticker}: {exc}", exc_info=True)
            qa_report["tickers_failed"].append(ticker)
            qa_report["per_ticker"][ticker] = {"status": "ERROR", "error": str(exc)}
            print(f"  \033[31m✗\033[0m  {ticker:<6} — unexpected error: {exc}")

    if frames:
        consolidated_df, all_path = save_consolidated(frames, output_dir)
        qa_report["consolidated_rows"] = len(consolidated_df)
        logger.info(f"Consolidated: {all_path} ({len(consolidated_df)} rows)")
        print(f"\n  Consolidated — {len(consolidated_df)} rows → {all_path}")
    else:
        logger.error("No valid data — consolidated file not created.")
        print("\n  No valid data available for consolidation.")

    qa_path = save_qa_report(qa_report, output_dir)
    logger.info(f"QA report: {qa_path}")

    n_ok   = len(qa_report["tickers_processed"])
    n_fail = len(qa_report["tickers_failed"])
    n_warn = sum(
        1 for t in qa_report["tickers_processed"]
        if qa_report["per_ticker"].get(t, {}).get("status") == "WARNING"
    )

    print(f"\nDone — {n_ok - n_warn} OK, {n_warn} warning(s), {n_fail} failed.")
    if qa_report["tickers_failed"]:
        print(f"  Failed: {qa_report['tickers_failed']}")

    if n_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()