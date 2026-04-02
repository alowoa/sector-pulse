"""
SectorPulse C13 — Text → Sector Mapping Pipeline (Level 1 + Level 2)

Maps each headline to 0, 1, or 2 GICS sectors using:
  - Level 0 : pre-tagged (Finnhub already populated gics_sector)
  - Level 1 : ticker / company-name matching
  - Level 2 : phrase > primary keyword > secondary keyword matching

Each article receives:
  article_id    – 0-based row index in the source CSV
  secteur_1     – best-matching sector (or empty)
  secteur_2     – runner-up sector (or empty)
  méthode_mapping – signal that drove secteur_1
  confiance     – confidence score [0.0 – 1.0]

Usage:
  uv run python scripts/map_sector.py
  uv run python scripts/map_sector.py --input data/raw/headlines.csv
  uv run python scripts/map_sector.py --output data/raw/headlines_sector_mapped.csv
  uv run python scripts/map_sector.py --min-conf 0.4
"""

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

DATA_DIR       = Path(__file__).parent.parent / "data"
KEYWORDS_FILE  = DATA_DIR / "sector_keywords.json"
C02_ALL_FILE   = DATA_DIR / "C02_mapping_secteur_ETF_tous_tickers-2.csv"
DEFAULT_INPUT  = DATA_DIR / "raw" / "headlines.csv"
DEFAULT_OUTPUT = DATA_DIR / "raw" / "headlines_sector_mapped.csv"

# Confidence tiers
CONF_PRETAG    = 1.00
CONF_TICKER    = 0.90
CONF_COMPANY   = 0.90
CONF_PHRASE    = 0.85
CONF_PRIMARY   = 0.70
CONF_SECONDARY = 0.40
CONF_MULTI_BOOST = 0.10   # applied when ≥ 2 independent signal types agree

# Short tickers (≤ 2 chars) require ALL-CAPS match in original text to avoid
# matching common English words ("at", "in", "be", "so", "do", "go", "ba", etc.)
SHORT_TICKER_MAX_LEN = 2

# Company name fragments shorter than this are skipped (too ambiguous)
COMPANY_NAME_MIN_LEN = 8

# Generic English words that appear as company name prefixes but are too
# ambiguous to use as match terms (e.g. "Capital One" → "Capital")
_GENERIC_NAME_TOKENS = frozenset({
    "american", "capital", "first", "general", "national", "digital",
    "united", "global", "international", "western", "southern", "northern",
    "eastern", "central", "federal", "pioneer", "guardian", "liberty",
    "alliance", "frontier", "crown", "summit", "heritage", "enterprise",
    "legacy", "foundation", "sterling", "charter", "commonwealth", "empire",
    "continental", "pacific", "atlantic", "standard", "universal",
})

_CORP_SUFFIXES = re.compile(
    r"\b(Inc\.?|Corp\.?|Corporation|Incorporated|plc|Ltd\.?|Limited|LLC|"
    r"Co\.?|Company|Group|Holdings?|International|Technologies|Technology|"
    r"Enterprises|Partners|Services|Solutions|N\.V\.?|S\.A\.?|AG|SE|GmbH|"
    r"and\s+Co\.?|Associates)\b\.?",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SectorSpec:
    name: str
    etf_ticker: str
    ticker_patterns:  list[re.Pattern] = field(default_factory=list)
    company_patterns: list[re.Pattern] = field(default_factory=list)
    phrase_patterns:  list[re.Pattern] = field(default_factory=list)
    primary_patterns: list[re.Pattern] = field(default_factory=list)
    secondary_patterns: list[re.Pattern] = field(default_factory=list)
    exclude_patterns: list[re.Pattern] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _word_boundary(word: str, case_insensitive: bool = True) -> re.Pattern:
    flags = re.IGNORECASE if case_insensitive else 0
    return re.compile(r"\b" + re.escape(word) + r"\b", flags)


def _ticker_pattern(ticker: str) -> re.Pattern | None:
    """
    Return a compiled pattern for the ticker.
    Short tickers (≤ SHORT_TICKER_MAX_LEN chars) are matched as ALL-CAPS only
    to prevent false positives against common English words.
    """
    t = ticker.strip()
    if not t:
        return None
    # Normalise BRK.B / BRK-B variations
    t_escaped = re.escape(t).replace(r"\.", r"[.\-]").replace(r"\-", r"[.\-]")
    if len(t) <= SHORT_TICKER_MAX_LEN:
        # Upper-case only; no IGNORECASE flag
        return re.compile(r"\b" + t_escaped + r"\b")
    return re.compile(r"\b" + t_escaped + r"\b", re.IGNORECASE)


def _strip_corp_suffix(name: str) -> str:
    """Remove boilerplate legal suffixes from a company name."""
    return _CORP_SUFFIXES.sub("", name).strip(" .,")


def _company_fragment(full_name: str) -> str | None:
    """
    Extract a matchable fragment from a company's full legal name.

    Strategy: use the full stripped name (up to 3 words).  Single-token
    generic words (e.g. "Capital", "American", "Digital") are rejected to
    avoid false positives against common English in headlines.

    Returns None if the result is too short or too generic.
    """
    cleaned = _strip_corp_suffix(full_name)
    words = cleaned.split()
    if not words:
        return None

    # Reject if the only distinctive token is a generic English word
    first = words[0].lower()
    if first in _GENERIC_NAME_TOKENS:
        if len(words) < 2:
            return None
        # Use both words; the second word provides specificity
        fragment = " ".join(words[:2])
    else:
        # Use the full cleaned name (up to 3 words for readability)
        fragment = " ".join(words[:3])

    if len(fragment) < COMPANY_NAME_MIN_LEN:
        return None
    return fragment


# ---------------------------------------------------------------------------
# Build sector specs
# ---------------------------------------------------------------------------

def _load_sector_specs(
    keywords_path: Path,
    c02_path: Path,
) -> list[SectorSpec]:
    with keywords_path.open() as f:
        kw_data = json.load(f)

    # Build {sector_name → [company_name_fragment]} from the C02 all-holdings file
    c02_companies: dict[str, list[str]] = {}
    if c02_path.exists():
        c02_df = pd.read_csv(c02_path)
        for _, row in c02_df.iterrows():
            frag = _company_fragment(str(row.get("nom_holding", "")))
            if frag:
                c02_companies.setdefault(str(row["secteur_gics"]), []).append(frag)
    else:
        print(f"  ⚠  {c02_path.name} not found — company-name matching disabled")

    specs: list[SectorSpec] = []
    for s in kw_data["sectors"]:
        sector_name = s["sector_name"]
        spec = SectorSpec(name=sector_name, etf_ticker=s["etf_ticker"])

        # --- Ticker patterns (ETF + named holdings) ---
        all_tickers = [s["etf_ticker"]] + s.get("tickers", [])
        for t in all_tickers:
            pat = _ticker_pattern(t)
            if pat:
                spec.ticker_patterns.append(pat)

        # --- Company name patterns (from C02 CSV) ---
        seen_frags: set[str] = set()
        for frag in c02_companies.get(sector_name, []):
            key = frag.lower()
            if key not in seen_frags:
                seen_frags.add(key)
                spec.company_patterns.append(_word_boundary(frag))

        # --- Phrase patterns (multi-word, high specificity) ---
        for phrase in s.get("phrases", []):
            spec.phrase_patterns.append(
                re.compile(re.escape(phrase), re.IGNORECASE)
            )

        # --- Primary keyword patterns ---
        for kw in s.get("keywords_primary", []):
            spec.primary_patterns.append(_word_boundary(kw))

        # --- Secondary keyword patterns ---
        for kw in s.get("keywords_secondary", []):
            spec.secondary_patterns.append(_word_boundary(kw))

        # --- Exclude terms (veto) ---
        for entry in s.get("exclude_terms", []):
            term = entry.get("term", "")
            if term:
                spec.exclude_patterns.append(
                    re.compile(re.escape(term), re.IGNORECASE)
                )

        specs.append(spec)

    return specs


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_sector(spec: SectorSpec, text: str, text_orig: str) -> tuple[float, str]:
    """
    Score a single sector against article text.

    `text`      – lowercased version (used for keyword/phrase matching)
    `text_orig` – original casing (used for short-ticker matching)

    Returns (confidence, method_label).
    method_label describes the dominant signal type.
    """
    # 1. Veto check: if any exclude term appears, this sector scores 0
    for pat in spec.exclude_patterns:
        if pat.search(text):
            return 0.0, "none"

    signals: list[tuple[float, str]] = []  # (score, method)

    # 2. Ticker matching (short tickers use original casing)
    for pat in spec.ticker_patterns:
        # Short-ticker patterns have no IGNORECASE flag → test against original
        target = text_orig if not (pat.flags & re.IGNORECASE) else text
        if pat.search(target):
            signals.append((CONF_TICKER, "level1_ticker"))
            break  # one ticker hit is enough; don't over-count

    # 3. Company name matching
    for pat in spec.company_patterns:
        if pat.search(text):
            signals.append((CONF_COMPANY, "level1_company"))
            break

    # 4. Phrase matching (checked before single keywords)
    for pat in spec.phrase_patterns:
        if pat.search(text):
            signals.append((CONF_PHRASE, "level2_phrase"))
            break

    # 5. Primary keywords
    for pat in spec.primary_patterns:
        if pat.search(text):
            signals.append((CONF_PRIMARY, "level2_primary"))
            break

    # 6. Secondary keywords (only if no stronger signal found yet)
    if not signals:
        for pat in spec.secondary_patterns:
            if pat.search(text):
                signals.append((CONF_SECONDARY, "level2_secondary"))
                break

    if not signals:
        return 0.0, "none"

    # Pick the highest-confidence signal as the dominant method
    best_score, best_method = max(signals, key=lambda x: x[0])

    # Multi-signal boost when ≥ 2 distinct signal types agree
    distinct_methods = {m for _, m in signals}
    if len(distinct_methods) >= 2:
        best_score = min(1.0, best_score + CONF_MULTI_BOOST)
        best_method = "multi"

    return best_score, best_method


# ---------------------------------------------------------------------------
# Map single article
# ---------------------------------------------------------------------------

def _map_article(
    row: pd.Series,
    specs: list[SectorSpec],
    min_conf: float,
) -> dict:
    # Level 0: already tagged by Finnhub
    raw_pretag = row.get("gics_sector", "")
    # Guard against pandas NaN (float) and explicit "nan" string
    if raw_pretag != raw_pretag or str(raw_pretag).lower() in ("nan", "none", ""):
        pretag = ""
    else:
        pretag = str(raw_pretag).strip()
    if pretag and pretag != "General":
        return {
            "secteur_1": pretag,
            "secteur_2": "",
            "méthode_mapping": "pre_tagged",
            "confiance": CONF_PRETAG,
        }

    title = str(row.get("title", "") or "")
    body  = str(row.get("body",  "") or "")
    text_orig = f"{title} {body}".strip()
    text_lower = text_orig.lower()

    # Score every sector
    scores: list[tuple[float, str, str]] = []  # (score, sector_name, method)
    for spec in specs:
        score, method = _score_sector(spec, text_lower, text_orig)
        if score > 0:
            scores.append((score, spec.name, method))

    # Sort descending
    scores.sort(key=lambda x: x[0], reverse=True)

    # Apply minimum confidence threshold
    scores = [s for s in scores if s[0] >= min_conf]

    if not scores:
        return {
            "secteur_1": "",
            "secteur_2": "",
            "méthode_mapping": "none",
            "confiance": 0.0,
        }

    best_score, best_sector, best_method = scores[0]
    second_sector = scores[1][1] if len(scores) > 1 else ""

    return {
        "secteur_1": best_sector,
        "secteur_2": second_sector,
        "méthode_mapping": best_method,
        "confiance": round(best_score, 4),
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(input_path: Path, output_path: Path, min_conf: float) -> None:
    print("=" * 60)
    print("  SectorPulse — Sector Mapping (C13)")
    print("=" * 60)

    # Load data
    print(f"\n  Loading keywords  : {KEYWORDS_FILE}")
    print(f"  Loading C02 data  : {C02_ALL_FILE}")
    specs = _load_sector_specs(KEYWORDS_FILE, C02_ALL_FILE)
    print(f"  Sectors loaded    : {len(specs)}")

    print(f"\n  Loading headlines : {input_path}")
    df = pd.read_csv(input_path)
    print(f"  Headlines         : {len(df)}")

    # Map each article
    print("\n  Mapping…")
    results = []
    for idx, row in df.iterrows():
        res = _map_article(row, specs, min_conf)
        results.append({
            "article_id": idx,
            **res,
        })

    out_df = pd.DataFrame(results)

    # Stats
    total = len(out_df)
    assigned = (out_df["secteur_1"] != "").sum()
    pretag   = (out_df["méthode_mapping"] == "pre_tagged").sum()
    has_two  = ((out_df["secteur_1"] != "") & (out_df["secteur_2"] != "")).sum()
    no_match = (out_df["secteur_1"] == "").sum()

    print(f"\n── Results ──────────────────────────────────────────────")
    print(f"  Total articles   : {total}")
    print(f"  Assigned (≥1 sec): {assigned}  ({100 * assigned / total:.1f}%)")
    print(f"    Pre-tagged     : {pretag}")
    print(f"    Mapped         : {assigned - pretag}")
    print(f"  Two sectors      : {has_two}  ({100 * has_two / total:.1f}%)")
    print(f"  No match         : {no_match}  ({100 * no_match / total:.1f}%)")

    # Method breakdown
    method_counts = out_df["méthode_mapping"].value_counts()
    print(f"\n── By method ────────────────────────────────────────────")
    for method, cnt in method_counts.items():
        print(f"  {method:<22} {cnt:>6}  ({100 * cnt / total:.1f}%)")

    # Sector breakdown
    assigned_df = out_df[out_df["secteur_1"] != ""]
    sector_counts = assigned_df["secteur_1"].value_counts()
    print(f"\n── By sector (secteur_1) ────────────────────────────────")
    for sector, cnt in sector_counts.items():
        print(f"  {sector:<28} {cnt:>6}")

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)
    print(f"\n  Output written    : {output_path}  ({len(out_df)} rows)")

    # DoD check
    rate = assigned / total
    print(f"\n── Definition of Done ───────────────────────────────────")
    if rate >= 0.50:
        print(f"  ✓  Attribution rate ≥ 50% : {100 * rate:.1f}%")
    else:
        print(f"  ⚠  Attribution rate < 50% : {100 * rate:.1f}%  (target ≥ 50%)")
        print(f"     Tip: lower --min-conf, add keywords, or expand ticker list.")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map headlines to GICS sectors (Level 1 + Level 2).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        metavar="PATH",
        help=f"Input headlines CSV (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        metavar="PATH",
        help=f"Output mapped CSV (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--min-conf",
        type=float,
        default=0.30,
        metavar="FLOAT",
        help="Minimum confidence to assign a sector (default: 0.30).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.input, args.output, args.min_conf)
