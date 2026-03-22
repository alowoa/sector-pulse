"""
Testing headline ingestion from RSS feeds and NewsAPI.org
"""

import csv
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuration des sources ────────────────────────────────────────────────

NEWSAPI_KEY = os.getenv("NEWSAPI_ORG_API_KEY", "")

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

TIMEOUT = 10  # secondes

# ── Helpers ──────────────────────────────────────────────────────────────────

def green(s):  return f"\033[92m{s}\033[0m"
def red(s):    return f"\033[91m{s}\033[0m"
def yellow(s): return f"\033[93m{s}\033[0m"
def bold(s):   return f"\033[1m{s}\033[0m"


def parse_rss(content: bytes) -> list[dict]:
    """Parse un flux RSS/Atom et retourne une liste de dicts."""
    headlines = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        return []

    # Détecte le namespace Atom si présent
    ns = {}
    if root.tag.startswith("{"):
        ns_uri = root.tag.split("}")[0].lstrip("{")
        ns = {"atom": ns_uri}

    # RSS 2.0
    for item in root.iter("item"):
        title_el = item.find("title")
        link_el  = item.find("link")
        date_el  = item.find("pubDate")
        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        link  = link_el.text.strip()  if link_el  is not None and link_el.text  else ""
        date  = date_el.text.strip()  if date_el  is not None and date_el.text  else ""
        if title:
            headlines.append({"title": title, "url": link, "published": date})

    # Atom (si aucun item RSS trouvé)
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
                headlines.append({"title": title, "url": link, "published": date})

    return headlines


# ── Tests RSS ────────────────────────────────────────────────────────────────

def test_rss_sources() -> list[dict]:
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
                print(f"  {yellow('⚠')} {name:<30}  Flux parsé mais 0 headlines extraits")
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
        print(f"  {yellow('⚠')} Clé NEWSAPI_KEY non renseignée dans .env — test ignoré")
        print(f"      Obtenez une clé gratuite sur : https://newsapi.org/register")
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
                        "title":     a.get("title", ""),
                        "url":       a.get("url", ""),
                        "published": a.get("publishedAt", ""),
                        "source":    f"NewsAPI / {a.get('source', {}).get('name', '')}",
                    })
            else:
                code = data.get("code", "")
                msg  = data.get("message", "")
                print(f"  {red('✗')} Query '{query}'  →  {code}: {msg}")
        except Exception as e:
            print(f"  {red('✗')} Query '{query}'  →  {type(e).__name__}: {e}")

    return all_headlines


# ── Rapport final ────────────────────────────────────────────────────────────

def print_limits():
    print(bold("\n── Limites du plan gratuit NewsAPI ─────────────────────"))
    limits = [
        ("Requêtes / jour",         "100"),
        ("Historique max",          "1 mois glissant"),
        ("Usage commercial",        "Non autorisé"),
        ("Articles par requête",    "100 max (pageSize)"),
        ("Sources disponibles",     "~80 000 sources mondiales"),
        ("Rate limiting",           "Pas de limite par minute documentée"),
    ]
    for label, value in limits:
        print(f"  {label:<30} {value}")
    print()
    print("  Plan Developer (gratuit) : https://newsapi.org/pricing")
    print("  Alternative si quota insuffisant : GNews API (100 req/j gratuit)")
    print("  Alternative open-source : RSS feeds uniquement (pas de quota)")


def save_csv(headlines: list[dict], path: str):
    if not headlines:
        return
    fieldnames = ["title", "source", "url", "published"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(headlines)
    print(f"\n  {green('✓')} {len(headlines)} headlines sauvegardées → {path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(bold("=" * 60))
    print(bold("  SectorPulse — C04 : Test des sources de headlines"))
    print(bold(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}"))
    print(bold("=" * 60))

    rss_headlines, rss_results = test_rss_sources()
    newsapi_headlines           = test_newsapi()

    all_headlines = rss_headlines + newsapi_headlines

    print_limits()

    # Résumé
    print(bold("\n── Résumé ───────────────────────────────────────────────"))
    ok      = sum(1 for r in rss_results if r["status"] == "OK")
    total   = len(rss_results)
    newsapi = "✓" if newsapi_headlines else "✗ (clé manquante)"
    print(f"  RSS opérationnels    : {ok}/{total}")
    print(f"  NewsAPI              : {newsapi}")
    print(f"  Headlines collectées : {len(all_headlines)}")

    dod_rss     = "✓" if ok >= 3 else "✗"
    dod_volume  = "✓" if len(all_headlines) >= 10 else "✗"
    print(f"\n  DoD C04 — ≥3 sources RSS OK   : {dod_rss}")
    print(f"  DoD C04 — ≥10 headlines total : {dod_volume}")

    # Sauvegarde
    out_path = os.path.join(os.path.dirname(__file__), "headlines_sample.csv")
    save_csv(all_headlines, out_path)

    print()
    if ok >= 3 and len(all_headlines) >= 10:
        print(green("  ✓ C04 Definition of Done atteinte — sources opérationnelles"))
    else:
        print(yellow("  ⚠ DoD partielle — vérifiez les sources en erreur ci-dessus"))


if __name__ == "__main__":
    main()