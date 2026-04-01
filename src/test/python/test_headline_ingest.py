"""
Testing headline ingestion from RSS feeds, NewsAPI.org, and Finnhub.

Run:
  uv run python src/test/python/test_headline_ingest.py

Output is written to both the terminal and test_headline_ingest.out.txt (same directory)
so results can be reviewed without re-running the script.
"""

import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from time import sleep

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuration des sources ────────────────────────────────────────────────

NEWSAPI_KEY     = os.getenv("NEWSAPI_ORG_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

FINNHUB_BASE  = "https://finnhub.io/api/v1"
FINNHUB_DELAY = 1.1  # seconds between calls — stays safely under 60 req/min

# All 11 GICS sectors mapped to their SPDR sector ETF ticker
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

RSS_SOURCES = {
    "Reuters (Business)":    "https://feeds.reuters.com/reuters/businessNews",
    "Reuters (Technology)":  "https://feeds.reuters.com/reuters/technologyNews",
    "CNBC (Top News)":       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "CNBC (Finance)":        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "MarketWatch (Top)":     "https://feeds.marketwatch.com/marketwatch/topstories/",
    "MarketWatch (Market)":  "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "Seeking Alpha":         "https://seekingalpha.com/feed.xml",
    "Benzinga":              "https://www.benzinga.com/feed",
    "Investing.com (US)":    "https://www.investing.com/rss/news_25.rss",
}

NEWSAPI_QUERIES = [
    "stock market sector",
    "S&P 500 ETF",
    "financial earnings",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SectorPulse/1.0; "
        "+https://github.com/sectorpulse)"
    )
}

TIMEOUT = 10


# ── Tee: stdout → console + plain-text file ──────────────────────────────────

class Tee:
    """Write to both console (with ANSI colours) and a file (ANSI stripped)."""
    _ANSI_RE = re.compile(r'\033\[[0-9;]*m')

    def __init__(self, console, file_handle):
        self.console = console
        self.file    = file_handle

    def write(self, text):
        self.console.write(text)
        self.file.write(self._ANSI_RE.sub('', text))

    def flush(self):
        self.console.flush()
        self.file.flush()


# ── Helpers ──────────────────────────────────────────────────────────────────

def green(s):  return f"\033[92m{s}\033[0m"
def red(s):    return f"\033[91m{s}\033[0m"
def yellow(s): return f"\033[93m{s}\033[0m"
def bold(s):   return f"\033[1m{s}\033[0m"


def parse_rss(content: bytes) -> list[dict]:
    """Parse a RSS/Atom feed and return a list of headline dicts."""
    headlines = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    # RSS 2.0
    for item in root.iter("item"):
        title_el = item.find("title")
        link_el  = item.find("link")
        date_el  = item.find("pubDate")
        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        link  = link_el.text.strip()  if link_el  is not None and link_el.text  else ""
        date  = date_el.text.strip()  if date_el  is not None and date_el.text  else ""
        if title:
            headlines.append({"title": title, "url": link, "published": date, "gics_sector": ""})

    # Atom (fallback if no RSS items found)
    if not headlines:
        atom_ns = "http://www.w3.org/2005/Atom"
        for entry in root.iter(f"{{{atom_ns}}}entry"):
            title_el = entry.find(f"{{{atom_ns}}}title")
            link_el  = entry.find(f"{{{atom_ns}}}link")
            date_el  = entry.find(f"{{{atom_ns}}}updated")
            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            link  = (link_el.get("href", "") if link_el is not None else "")
            date  = date_el.text.strip()  if date_el  is not None and date_el.text  else ""
            if title:
                headlines.append({"title": title, "url": link, "published": date, "gics_sector": ""})

    return headlines


# ── Tests RSS ────────────────────────────────────────────────────────────────

def test_rss_sources() -> tuple[list[dict], list[dict]]:
    print(bold("\n── RSS Feeds ────────────────────────────────────────────"))
    all_headlines = []
    results = []

    for name, url in RSS_SOURCES.items():
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            headlines = parse_rss(resp.content)
            count = len(headlines)
            if count > 0:
                print(f"  {green('✓')} {name:<30}  {count} headlines")
                for h in headlines[:3]:
                    print(f"       → {h['title'][:80]}")
                for h in headlines[:10]:
                    h["source"] = name
                    all_headlines.append(h)
                results.append({"source": name, "url": url, "status": "OK", "count": count})
            else:
                print(f"  {yellow('⚠')} {name:<30}  Feed parsed but 0 headlines extracted")
                results.append({"source": name, "url": url, "status": "EMPTY", "count": 0})
        except requests.exceptions.Timeout:
            print(f"  {red('✗')} {name:<30}  Timeout ({TIMEOUT}s)")
            results.append({"source": name, "url": url, "status": "TIMEOUT", "count": 0})
        except requests.exceptions.HTTPError as e:
            print(f"  {red('✗')} {name:<30}  HTTP {e.response.status_code}")
            results.append({"source": name, "url": url, "status": f"HTTP_{e.response.status_code}", "count": 0})
        except Exception as e:
            print(f"  {red('✗')} {name:<30}  {type(e).__name__}: {e}")
            results.append({"source": name, "url": url, "status": "ERROR", "count": 0})

    return all_headlines, results


# ── Test NewsAPI ─────────────────────────────────────────────────────────────

def test_newsapi() -> list[dict]:
    print(bold("\n── NewsAPI ──────────────────────────────────────────────"))
    all_headlines = []

    if not NEWSAPI_KEY or NEWSAPI_KEY == "your_key_here":
        print(f"  {yellow('⚠')} NEWSAPI_ORG_API_KEY not set in .env — skipping")
        print(f"      Get a free key at: https://newsapi.org/register")
        return []

    base_url = "https://newsapi.org/v2/everything"
    for query in NEWSAPI_QUERIES:
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 10,
            "apiKey": NEWSAPI_KEY,
        }
        try:
            resp = requests.get(base_url, params=params, timeout=TIMEOUT)
            data = resp.json()
            if data.get("status") == "ok":
                articles = data.get("articles", [])
                print(f"  {green('✓')} Query '{query}'  →  {len(articles)} articles")
                for a in articles[:3]:
                    print(f"       → {a.get('title', '')[:80]}")
                for a in articles:
                    all_headlines.append({
                        "title":       a.get("title", ""),
                        "url":         a.get("url", ""),
                        "published":   a.get("publishedAt", ""),
                        "source":      f"NewsAPI / {a.get('source', {}).get('name', '')}",
                        "gics_sector": "",
                    })
            else:
                code = data.get("code", "")
                msg  = data.get("message", "")
                print(f"  {red('✗')} Query '{query}'  →  {code}: {msg}")
        except Exception as e:
            print(f"  {red('✗')} Query '{query}'  →  {type(e).__name__}: {e}")

    return all_headlines


# ── Test Finnhub ─────────────────────────────────────────────────────────────

def test_finnhub() -> tuple[list[dict], dict[str, int]]:
    """
    Query Finnhub for:
      - General market news (1 call)
      - Company news for each of the 11 GICS sector ETFs (11 calls)

    Rate limit: 60 req/min on the free tier.
    Strategy: sleep FINNHUB_DELAY seconds after each call (12 calls total ≈ 13s).
    """
    print(bold("\n── Finnhub ──────────────────────────────────────────────"))

    if not FINNHUB_API_KEY:
        print(f"  {yellow('⚠')} FINNHUB_API_KEY not set in .env — skipping")
        print(f"      Get a free key at: https://finnhub.io/register")
        return [], {}

    all_headlines: list[dict] = []
    sector_counts: dict[str, int] = {}
    calls_made = 0

    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date   = datetime.now().strftime("%Y-%m-%d")

    # — General market news (1 call) —
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/news",
            params={"category": "general", "token": FINNHUB_API_KEY},
            timeout=TIMEOUT,
        )
        calls_made += 1
        data = resp.json()
        if isinstance(data, list) and data:
            general_items = [
                {
                    "title":       a.get("headline", ""),
                    "url":         a.get("url", ""),
                    "published":   datetime.fromtimestamp(a["datetime"]).strftime("%Y-%m-%d")
                                   if a.get("datetime") else "",
                    "source":      f"Finnhub / {a.get('source', '')}",
                    "gics_sector": "General",
                }
                for a in data if a.get("headline")
            ]
            sector_counts["General"] = len(general_items)
            all_headlines.extend(general_items)
            print(f"  {green('✓')} {'General market news':<32}  {len(general_items):>3} articles")
            for a in general_items[:3]:
                print(f"       → {a['title'][:80]}")
        else:
            sector_counts["General"] = 0
            print(f"  {yellow('⚠')} {'General market news':<32}    0 articles")
    except Exception as e:
        sector_counts["General"] = 0
        print(f"  {red('✗')} {'General market news':<32}  {type(e).__name__}: {e}")

    sleep(FINNHUB_DELAY)

    # — Per-sector ETF news (11 calls) —
    for sector, ticker in GICS_SECTORS.items():
        label = f"{ticker} ({sector})"
        try:
            resp = requests.get(
                f"{FINNHUB_BASE}/company-news",
                params={
                    "symbol": ticker,
                    "from":   from_date,
                    "to":     to_date,
                    "token":  FINNHUB_API_KEY,
                },
                timeout=TIMEOUT,
            )
            calls_made += 1
            data = resp.json()
            if isinstance(data, list) and data:
                items = [
                    {
                        "title":       a.get("headline", ""),
                        "url":         a.get("url", ""),
                        "published":   datetime.fromtimestamp(a["datetime"]).strftime("%Y-%m-%d")
                                       if a.get("datetime") else "",
                        "source":      f"Finnhub / {a.get('source', '')}",
                        "gics_sector": sector,
                    }
                    for a in data if a.get("headline")
                ]
                sector_counts[sector] = len(items)
                all_headlines.extend(items)
                print(f"  {green('✓')} {label:<32}  {len(items):>3} articles")
                for a in items[:3]:
                    print(f"       → {a['title'][:80]}")
            else:
                sector_counts[sector] = 0
                print(f"  {yellow('⚠')} {label:<32}    0 articles")
        except Exception as e:
            sector_counts[sector] = 0
            print(f"  {red('✗')} {label:<32}  {type(e).__name__}: {e}")

        sleep(FINNHUB_DELAY)

    print(f"\n  API calls used this run : {calls_made}  (free limit: 60/min, no daily cap)")
    print(f"  Date window             : {from_date}  →  {to_date}  (last 30 days)")
    return all_headlines, sector_counts


# ── API limits reference ─────────────────────────────────────────────────────

def print_api_limits():
    print(bold("\n── API Limits Reference ─────────────────────────────────"))

    print(bold("  NewsAPI (newsapi.org)"))
    newsapi_limits = [
        ("Requests / day",      "100"),
        ("History",             "~1 month (free plan)"),
        ("Articles / request",  "100 max (pageSize)"),
        ("Commercial use",      "Not allowed on free"),
    ]
    for label, value in newsapi_limits:
        print(f"    {label:<28} {value}")

    print()
    print(bold("  Finnhub (finnhub.io)"))
    finnhub_limits = [
        ("Requests / minute",   "60 (free plan)"),
        ("Daily cap",           "None documented"),
        ("History (company)",   "~2 years"),
        ("History (market)",    "Current feed only"),
        ("Commercial use",      "Allowed on free"),
    ]
    for label, value in finnhub_limits:
        print(f"    {label:<28} {value}")

    print()
    print(f"  Finnhub free key : https://finnhub.io/register")
    print(f"  NewsAPI free key : https://newsapi.org/register")


# ── Comparison table ─────────────────────────────────────────────────────────

def print_comparison(
    rss_results: list[dict],
    newsapi_headlines: list[dict],
    sector_counts: dict[str, int],
):
    print(bold("\n── Source Comparison ────────────────────────────────────"))
    print(f"  {'Source':<36}  {'Articles':>8}  GICS coverage")
    print(f"  {'-'*36}  {'-'*8}  {'-'*22}")

    rss_ok    = sum(r["count"] for r in rss_results if r["status"] == "OK")
    rss_total = sum(r["count"] for r in rss_results)
    print(f"  {'RSS feeds (all sources)':<36}  {rss_total:>8}  n/a (unsectorised)")

    newsapi_count = len(newsapi_headlines)
    na_str = "n/a (generic queries)" if newsapi_count else "skipped (no key)"
    print(f"  {'NewsAPI (3 generic queries)':<36}  {newsapi_count:>8}  {na_str}")

    if sector_counts:
        general_count = sector_counts.get("General", 0)
        print(f"  {'Finnhub — General market':<36}  {general_count:>8}  n/a")
        for sector, ticker in GICS_SECTORS.items():
            count = sector_counts.get(sector, 0)
            icon  = green("✓") if count > 0 else yellow("⚠")
            print(f"  {icon} {'Finnhub — ' + ticker + ' (' + sector + ')':<34}  {count:>8}  {sector}")
        finnhub_total    = sum(sector_counts.values())
        sectors_covered  = sum(1 for s in GICS_SECTORS if sector_counts.get(s, 0) > 0)
        print(f"  {'-'*36}  {'-'*8}")
        print(f"  {'Finnhub total':<36}  {finnhub_total:>8}  {sectors_covered}/11 GICS sectors")
    else:
        print(f"  {'Finnhub':<36}  {'skipped':>8}  (no key)")

    print()
    rss_ok_count = sum(1 for r in rss_results if r["status"] == "OK")
    rss_total_count = len(rss_results)
    print(f"  RSS sources operational : {rss_ok_count}/{rss_total_count}")


# ── CSV export ───────────────────────────────────────────────────────────────

def save_csv(headlines: list[dict], path: str):
    if not headlines:
        return
    fieldnames = ["title", "source", "url", "published", "gics_sector"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(headlines)
    print(f"\n  {green('✓')} {len(headlines)} headlines saved → {path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_path   = os.path.join(script_dir, "test_headline_ingest.out.txt")

    out_file   = open(out_path, "w", encoding="utf-8")
    sys.stdout = Tee(sys.__stdout__, out_file)

    try:
        print(bold("=" * 60))
        print(bold("  SectorPulse — C04 extended: RSS + NewsAPI + Finnhub"))
        print(bold(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}"))
        print(bold("=" * 60))

        rss_headlines, rss_results = test_rss_sources()
        newsapi_headlines          = test_newsapi()
        finnhub_headlines, sec_counts = test_finnhub()

        all_headlines = rss_headlines + newsapi_headlines + finnhub_headlines

        print_api_limits()
        print_comparison(rss_results, newsapi_headlines, sec_counts)

        csv_path = os.path.join(script_dir, "headlines_sample.csv")
        save_csv(all_headlines, csv_path)

        print()
        rss_ok = sum(1 for r in rss_results if r["status"] == "OK")
        if rss_ok >= 3 and len(all_headlines) >= 10:
            print(green("  ✓ C04 Definition of Done met — sources operational"))
        else:
            print(yellow("  ⚠ Partial DoD — check sources in error above"))

    finally:
        sys.stdout = sys.__stdout__
        out_file.close()

    print(f"\nOutput saved → {out_path}")


if __name__ == "__main__":
    main()
