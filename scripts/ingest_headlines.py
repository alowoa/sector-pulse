"""
Financial headlines ingestion script — SectorPulse C12.

Collects US financial headlines from:
  - RSS feeds (Reuters, CNBC, MarketWatch, Seeking Alpha, Benzinga, Investing.com)
  - Finnhub (free tier) queried per GICS sector ETF + top-5 holdings from C02 mapping

All headlines are stored in a single SQLite table `headlines` with a `provider` column
for data lineage (`finnhub_api` or `custom_rss_extraction`).
A combined CSV is exported after each run (data/raw/headlines.csv).

Finnhub free plan: 60 req/min, no daily cap, ~2 years of history.
This script makes 67 calls per run (~74s with rate-limit sleep).

Usage:
  uv run python scripts/ingest_headlines.py
  uv run python scripts/ingest_headlines.py --months 12
  uv run python scripts/ingest_headlines.py --start 2025-03-01 --end 2026-03-26
  uv run python scripts/ingest_headlines.py --no-rss
  uv run python scripts/ingest_headlines.py --no-finnhub
"""

import argparse
import html
import os
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import sleep

import pandas as pd
import requests
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RSS_SOURCES: dict[str, str] = {
    "Reuters (Business)":   "https://feeds.reuters.com/reuters/businessNews",
    "Reuters (Technology)": "https://feeds.reuters.com/reuters/technologyNews",
    "CNBC (Top News)":      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "CNBC (Finance)":       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "MarketWatch (Top)":    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "MarketWatch (Market)": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "Seeking Alpha":        "https://seekingalpha.com/feed.xml",
    "Benzinga":             "https://www.benzinga.com/feed",
    "Investing.com (US)":   "https://www.investing.com/rss/news_25.rss",
}

# All 11 GICS sectors → SPDR sector ETF ticker
GICS_SECTORS: dict[str, str] = {
    "Energy":                 "XLE",
    "Materials":              "XLB",
    "Industrials":            "XLI",
    "Consumer Discretionary": "XLY",
    "Consumer Staples":       "XLP",
    "Health Care":            "XLV",
    "Financials":             "XLF",
    "Information Technology": "XLK",
    "Communication Services": "XLC",
    "Utilities":              "XLU",
    "Real Estate":            "XLRE",
}

FINNHUB_BASE  = "https://finnhub.io/api/v1"
FINNHUB_DELAY = 1.1  # seconds between calls — stays safely under 60 req/min

DATA_DIR   = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "raw"
DB_FILE    = OUTPUT_DIR / "headlines.db"
CSV_FILE   = OUTPUT_DIR / "headlines.csv"

# C02 top-5 holdings per sector — loaded at runtime
C02_TOP5_FILE = DATA_DIR / "C02_mapping_secteur_ETF_top5_holdings.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SectorPulse/1.0; "
        "+https://github.com/sectorpulse)"
    )
}
TIMEOUT = 15


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS headlines (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    date        TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    url         TEXT    NOT NULL,
    body        TEXT,
    gics_sector TEXT,
    provider    TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL,
    UNIQUE(url, title)
)
"""

_OLD_TABLES = ("rss_extracted_headlines", "finnhub_headlines")


def get_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(_DDL)
    conn.commit()
    # Warn if old schema tables are still present
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    legacy = tables & set(_OLD_TABLES)
    if legacy:
        print(
            f"  \033[33m⚠\033[0m  Legacy tables detected: {', '.join(legacy)}\n"
            f"       Run `rm {db_path}` to start fresh with the new schema.",
            file=sys.stderr,
        )
    return conn


def insert_headlines(conn: sqlite3.Connection, rows: list[dict]) -> tuple[int, int]:
    """Insert rows into `headlines`; skip duplicates on (url, title)."""
    inserted = skipped = 0
    now = _utcnow()
    for row in rows:
        try:
            conn.execute(
                "INSERT INTO headlines "
                "(date, title, source, url, body, gics_sector, provider, ingested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row.get("date", ""),
                    row["title"],
                    row["source"],
                    row.get("url", ""),
                    row.get("body"),
                    row.get("gics_sector", ""),
                    row["provider"],
                    now,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    return inserted, skipped


def export_csv(conn: sqlite3.Connection, csv_path: Path) -> int:
    df = pd.read_sql_query(
        "SELECT date, title, source, url, body, gics_sector, provider "
        "FROM headlines ORDER BY date DESC",
        conn,
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    return len(df)


def db_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM headlines").fetchone()[0]


# ---------------------------------------------------------------------------
# RSS helpers
# ---------------------------------------------------------------------------

def _strip_tags(text: str) -> str:
    """Remove HTML tags and unescape HTML entities."""
    result = []
    in_tag = False
    for ch in text:
        if ch == "<":
            in_tag = True
        elif ch == ">":
            in_tag = False
        elif not in_tag:
            result.append(ch)
    return html.unescape("".join(result)).strip()


def _normalise_date(raw: str) -> str:
    """Parse a date string and return YYYY-MM-DD, or '' on failure."""
    if not raw:
        return ""
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return ""


def _in_window(date_str: str, start: date, end: date) -> bool:
    if not date_str:
        return True  # keep undated items
    try:
        return start <= date.fromisoformat(date_str[:10]) <= end
    except ValueError:
        return True


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_rss(content: bytes, source_name: str) -> list[dict]:
    """Parse RSS 2.0 or Atom feed into headline dicts."""
    items = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    atom_ns = "http://www.w3.org/2005/Atom"

    # RSS 2.0
    for item in root.iter("item"):
        def _text(tag):
            el = item.find(tag)
            return (el.text or "").strip() if el is not None else ""

        title = _text("title")
        if not title:
            continue
        raw_body = _text("description") or _text("summary")
        body = _strip_tags(raw_body)[:2000] if raw_body else None
        items.append({
            "title":       title,
            "url":         _text("link"),
            "date":        _normalise_date(_text("pubDate")),
            "source":      source_name,
            "body":        body,
            "gics_sector": "",
            "provider":    "custom_rss_extraction",
        })

    # Atom (fallback)
    if not items:
        for entry in root.iter(f"{{{atom_ns}}}entry"):
            def _atext(tag):
                el = entry.find(f"{{{atom_ns}}}{tag}")
                return (el.text or "").strip() if el is not None else ""

            title = _atext("title")
            if not title:
                continue
            link_el = entry.find(f"{{{atom_ns}}}link")
            url = link_el.get("href", "") if link_el is not None else ""
            raw_body = _atext("content") or _atext("summary")
            body = _strip_tags(raw_body)[:2000] if raw_body else None
            items.append({
                "title":       title,
                "url":         url,
                "date":        _normalise_date(_atext("updated") or _atext("published")),
                "source":      source_name,
                "body":        body,
                "gics_sector": "",
                "provider":    "custom_rss_extraction",
            })

    return items


# ---------------------------------------------------------------------------
# Ingestion — RSS
# ---------------------------------------------------------------------------

def ingest_rss(start: date, end: date) -> list[dict]:
    print("\n\033[1m── RSS Feeds ────────────────────────────────────────────\033[0m")
    all_items: list[dict] = []

    for name, url in RSS_SOURCES.items():
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            items = _parse_rss(resp.content, name)
            filtered = [i for i in items if _in_window(i["date"], start, end)]
            count = len(filtered)
            all_items.extend(filtered)
            icon = "\033[32m✓\033[0m" if count > 0 else "\033[33m⚠\033[0m"
            print(f"  {icon}  {name:<32}  {count:>3} articles")
        except requests.exceptions.Timeout:
            print(f"  \033[31m✗\033[0m  {name:<32}  timeout ({TIMEOUT}s)")
        except requests.exceptions.HTTPError as e:
            print(f"  \033[31m✗\033[0m  {name:<32}  HTTP {e.response.status_code}")
        except Exception as e:
            print(f"  \033[31m✗\033[0m  {name:<32}  {type(e).__name__}: {e}")

    return [i for i in all_items if i.get("title")]


# ---------------------------------------------------------------------------
# Ingestion — Finnhub
# ---------------------------------------------------------------------------

def _load_sector_top5(path: Path) -> dict[str, list[str]]:
    """Load {sector_name: [ticker, ...]} from C02 top-5 holdings CSV."""
    df = pd.read_csv(path)
    result: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        result.setdefault(row["secteur_gics"], []).append(row["ticker_holding"])
    return result


def _parse_finnhub_articles(data: list, gics_sector: str) -> list[dict]:
    items = []
    for a in data:
        headline = (a.get("headline") or "").strip()
        if not headline:
            continue
        body = (a.get("summary") or "").strip() or None
        ts = a.get("datetime")
        pub_date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else ""
        items.append({
            "title":       headline,
            "url":         a.get("url", ""),
            "date":        pub_date,
            "source":      f"Finnhub / {a.get('source', '')}".rstrip(" /"),
            "body":        body,
            "gics_sector": gics_sector,
            "provider":    "finnhub_api",
        })
    return items


def _finnhub_get(endpoint: str, params: dict, api_key: str) -> list:
    """GET a Finnhub endpoint; return list or [] on error."""
    resp = requests.get(
        f"{FINNHUB_BASE}/{endpoint}",
        params={**params, "token": api_key},
        timeout=TIMEOUT,
    )
    data = resp.json() if resp.ok else []
    return data if isinstance(data, list) else []


def ingest_finnhub(api_key: str, start: date, end: date) -> list[dict]:
    """
    API calls per run:
      1  — /news?category=general (broad market)
      11 — /company-news per GICS sector ETF
      55 — /company-news for top-5 holdings per sector (from C02_mapping file)
    Total: 67 calls × 1.1s ≈ 74s, well under 60 req/min limit.
    """
    print("\n\033[1m── Finnhub ──────────────────────────────────────────────\033[0m")
    all_items: list[dict] = []
    calls = 0

    from_str = start.isoformat()
    to_str   = end.isoformat()

    # Load top-5 holdings per sector from C02 mapping
    sector_top5: dict[str, list[str]] = {}
    if C02_TOP5_FILE.exists():
        try:
            sector_top5 = _load_sector_top5(C02_TOP5_FILE)
        except Exception as e:
            print(f"  \033[33m⚠\033[0m  Could not load {C02_TOP5_FILE.name}: {e} — ETF-only mode")
    else:
        print(f"  \033[33m⚠\033[0m  {C02_TOP5_FILE.name} not found — ETF-only mode")

    # — General market news —
    try:
        data = _finnhub_get("news", {"category": "general"}, api_key)
        calls += 1
        items = _parse_finnhub_articles(data, "General")
        all_items.extend(items)
        icon = "\033[32m✓\033[0m" if items else "\033[33m⚠\033[0m"
        print(f"  {icon}  {'General market':<36}  {len(items):>4} articles")
    except Exception as e:
        calls += 1
        print(f"  \033[31m✗\033[0m  {'General market':<36}  {type(e).__name__}: {e}")
    sleep(FINNHUB_DELAY)

    # — Per-sector ETF + top-5 holdings —
    for sector, etf_ticker in GICS_SECTORS.items():
        # ETF
        try:
            data = _finnhub_get("company-news", {"symbol": etf_ticker, "from": from_str, "to": to_str}, api_key)
            calls += 1
            items = _parse_finnhub_articles(data, sector)
            all_items.extend(items)
            icon = "\033[32m✓\033[0m" if items else "\033[33m⚠\033[0m"
            print(f"  {icon}  {etf_ticker + ' — ' + sector:<36}  {len(items):>4} articles")
        except Exception as e:
            calls += 1
            print(f"  \033[31m✗\033[0m  {etf_ticker + ' — ' + sector:<36}  {type(e).__name__}: {e}")
        sleep(FINNHUB_DELAY)

        # Top-5 holdings for this sector
        for stock_ticker in sector_top5.get(sector, []):
            try:
                data = _finnhub_get("company-news", {"symbol": stock_ticker, "from": from_str, "to": to_str}, api_key)
                calls += 1
                items = _parse_finnhub_articles(data, sector)
                all_items.extend(items)
                icon = "\033[32m✓\033[0m" if items else "\033[33m⚠\033[0m"
                print(f"  {icon}    {stock_ticker:<34}  {len(items):>4} articles")
            except Exception as e:
                calls += 1
                print(f"  \033[31m✗\033[0m    {stock_ticker:<34}  {type(e).__name__}: {e}")
            sleep(FINNHUB_DELAY)

    print(f"\n  API calls used this run : {calls}  (free limit: 60 req/min)")
    print(f"  Date window             : {from_str}  →  {to_str}")
    return [i for i in all_items if i.get("title")]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest financial headlines to SQLite + CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--months",
        type=int,
        default=12,
        metavar="N",
        help="Number of months back from today (default: 12).",
    )
    date_group.add_argument(
        "--start",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Explicit start date.",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument("--no-rss",     action="store_true", help="Skip RSS ingestion.")
    parser.add_argument("--no-finnhub", action="store_true", help="Skip Finnhub ingestion.")
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_FILE,
        metavar="PATH",
        help=f"SQLite database path (default: {DB_FILE}).",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=CSV_FILE,
        metavar="PATH",
        help=f"CSV export path (default: {CSV_FILE}).",
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
    load_dotenv()

    args = parse_args()
    start, end = resolve_dates(args)

    api_key = os.getenv("FINNHUB_API_KEY", "")
    if not args.no_finnhub and not api_key:
        print(
            "WARNING: FINNHUB_API_KEY not set — skipping Finnhub ingestion.\n"
            "         Add it to .env or pass --no-finnhub to suppress this warning.",
            file=sys.stderr,
        )
        args.no_finnhub = True

    if args.no_rss and args.no_finnhub:
        print("ERROR: both --no-rss and --no-finnhub set; nothing to ingest.", file=sys.stderr)
        sys.exit(1)

    print("\033[1m" + "=" * 60 + "\033[0m")
    print("\033[1m  SectorPulse — Headlines Ingestion (C12)\033[0m")
    print(f"\033[1m  Window : {start}  →  {end}\033[0m")
    print(f"\033[1m  DB     : {args.db}\033[0m")
    print("\033[1m" + "=" * 60 + "\033[0m")

    conn = get_db(args.db)
    count_before = db_count(conn)

    all_rows: list[dict] = []

    if not args.no_rss:
        all_rows.extend(ingest_rss(start, end))

    if not args.no_finnhub:
        all_rows.extend(ingest_finnhub(api_key, start, end))

    print(f"\n\033[1m── Storage ──────────────────────────────────────────────\033[0m")
    inserted, skipped = insert_headlines(conn, all_rows)
    count_after = db_count(conn)

    print(f"  Fetched this run : {len(all_rows)}")
    print(f"  Inserted (new)   : {inserted}")
    print(f"  Skipped (dupes)  : {skipped}")

    # Provider breakdown
    rows = conn.execute(
        "SELECT provider, COUNT(*) FROM headlines GROUP BY provider ORDER BY 2 DESC"
    ).fetchall()
    print(f"\n  {'Provider':<28}  {'Total in DB':>11}")
    print(f"  {'-'*28}  {'-'*11}")
    for provider, cnt in rows:
        print(f"  {provider:<28}  {cnt:>11}")
    print(f"  {'TOTAL':<28}  {count_after:>11}")

    total_csv = export_csv(conn, args.csv)
    print(f"\n  CSV exported : {args.csv}  ({total_csv} rows)")

    print(f"\n\033[1m── Definition of Done ───────────────────────────────────\033[0m")
    if count_after >= 3000:
        print(f"  \033[32m✓\033[0m  ≥3000 headlines in DB  : {count_after}")
    else:
        needed = 3000 - count_after
        print(f"  \033[33m⚠\033[0m  ≥3000 headlines in DB  : {count_after} / 3000  ({needed} more needed)")
        print(f"       Tip: run with --months 18 or longer to fetch more historical data.")

    conn.close()
    print()


if __name__ == "__main__":
    main()
