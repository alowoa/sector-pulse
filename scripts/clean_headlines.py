"""
SectorPulse C18 — Nettoyage et normalisation du corpus headlines

Opérations appliquées :
  1. Normalisation encodage  : NFKC Unicode + désescapage entités HTML (titre et body)
  2. Dédoublonnage          : suppression doublons sur titre normalisé (même article
                              ingéré plusieurs fois via requêtes Finnhub par secteur ETF).
                              En cas de doublon, on conserve la ligne avec le secteur GICS
                              le plus précis (secteur spécifique > 'General' > NaN).
  3. Filtrage hors-scope    : suppression des titres trop courts (< 10 caractères).

Sorties :
  - CSV nettoyé   : data/raw/headlines_clean.csv
  - Log rejets    : data/raw/headlines_rejection_log.csv
  - Statistiques  : affichées dans la console (nb avant/après, taux de rejet, répartition)

Usage :
  uv run python scripts/clean_headlines.py
  uv run python scripts/clean_headlines.py --input data/raw/headlines.csv
  uv run python scripts/clean_headlines.py --output data/raw/headlines_clean.csv
  uv run python scripts/clean_headlines.py --min-title-len 15
"""

import argparse
import html as html_lib
import sys
import unicodedata
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

DATA_DIR        = Path(__file__).parent.parent / "data"
DEFAULT_INPUT   = DATA_DIR / "raw" / "headlines.csv"
DEFAULT_OUTPUT  = DATA_DIR / "raw" / "headlines_clean.csv"
DEFAULT_LOG     = DATA_DIR / "raw" / "headlines_rejection_log.csv"

# Minimum title length (after stripping) to keep an article
DEFAULT_MIN_TITLE_LEN = 10

# Sector quality ranking for dedup: lower index = higher priority
_SECTOR_QUALITY = {
    None: 0,
    "nan": 0,
    "": 0,
    "General": 1,
}
# Any named GICS sector gets quality 2 (handled in _sector_priority())


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_text(text: str | None) -> str | None:
    """NFKC Unicode normalisation + HTML entity unescaping."""
    if text is None or (isinstance(text, float)):
        return None
    text = unicodedata.normalize("NFKC", str(text))
    text = html_lib.unescape(text)
    return text


def _title_fingerprint(title: str) -> str:
    """Lowercased, whitespace-collapsed title used for duplicate detection."""
    return " ".join(title.lower().split())


def _sector_priority(sector) -> int:
    """
    Returns a quality score for the gics_sector value.
    Higher score = more precise = preferred when deduplicating.
    """
    if pd.isna(sector) or str(sector).strip() in ("", "nan", "NaN"):
        return 0
    if str(sector).strip() == "General":
        return 1
    return 2


# ---------------------------------------------------------------------------
# Cleaning pipeline
# ---------------------------------------------------------------------------

def clean(
    df: pd.DataFrame,
    min_title_len: int = DEFAULT_MIN_TITLE_LEN,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply all cleaning steps.

    Returns:
        clean_df      : filtered and normalised DataFrame
        rejection_log : DataFrame with columns [original_index, title, reason]
    """
    rejections: list[dict] = []

    # ---- Step 1 : encoding normalisation (in-place, no rows dropped) --------
    df = df.copy()
    df["title"] = df["title"].apply(_normalise_text)
    df["body"]  = df["body"].apply(_normalise_text)

    # ---- Step 2 : deduplication on normalised title -------------------------
    df["_fingerprint"] = df["title"].apply(
        lambda t: _title_fingerprint(t) if isinstance(t, str) else ""
    )
    df["_sector_prio"] = df["gics_sector"].apply(_sector_priority)

    # Within each group of identical fingerprints, sort so the best row comes
    # first (highest sector priority, then lowest original index for stability).
    df["_orig_idx"] = df.index
    df = df.sort_values(
        ["_fingerprint", "_sector_prio", "_orig_idx"],
        ascending=[True, False, True],
        ignore_index=False,
    )

    dup_mask = df.duplicated(subset=["_fingerprint"], keep="first")
    for idx in df.index[dup_mask]:
        rejections.append({
            "original_index": idx,
            "title":          df.at[idx, "title"],
            "source":         df.at[idx, "source"],
            "gics_sector":    df.at[idx, "gics_sector"],
            "reason":         "duplicate_title",
        })
    df = df[~dup_mask]

    # Restore original row order after dedup sort
    df = df.sort_index()

    # ---- Step 3 : out-of-scope filtering (titre trop court) -----------------
    short_mask = df["title"].apply(
        lambda t: isinstance(t, str) and len(t.strip()) < min_title_len
    )
    for idx in df.index[short_mask]:
        rejections.append({
            "original_index": idx,
            "title":          df.at[idx, "title"],
            "source":         df.at[idx, "source"],
            "gics_sector":    df.at[idx, "gics_sector"],
            "reason":         "title_too_short",
        })
    df = df[~short_mask]

    # ---- Clean up internal columns -----------------------------------------
    df = df.drop(columns=["_fingerprint", "_sector_prio", "_orig_idx"])

    rejection_log = pd.DataFrame(
        rejections,
        columns=["original_index", "title", "source", "gics_sector", "reason"],
    )
    return df, rejection_log


# ---------------------------------------------------------------------------
# Statistics report
# ---------------------------------------------------------------------------

def print_stats(
    original: pd.DataFrame,
    cleaned: pd.DataFrame,
    rejection_log: pd.DataFrame,
) -> None:
    n_before = len(original)
    n_after  = len(cleaned)
    n_dropped = len(rejection_log)
    reject_rate = n_dropped / n_before * 100 if n_before else 0.0

    print()
    print("=" * 60)
    print("  C18 — Rapport de nettoyage du corpus headlines")
    print("=" * 60)
    print(f"  Articles avant nettoyage : {n_before:>6,}")
    print(f"  Articles après nettoyage : {n_after:>6,}")
    print(f"  Articles supprimés       : {n_dropped:>6,}  ({reject_rate:.1f} %)")
    print()

    # Rejections by reason
    if not rejection_log.empty:
        print("  Motifs de suppression :")
        for reason, count in rejection_log["reason"].value_counts().items():
            pct = count / n_before * 100
            print(f"    {reason:<25} {count:>5,}  ({pct:.1f} %)")
        print()

    # Source distribution before / after
    src_before = original["source"].value_counts()
    src_after  = cleaned["source"].value_counts()

    all_sources = src_before.index.union(src_after.index)
    print("  Répartition par source :")
    print(f"    {'Source':<35} {'Avant':>6}  {'Après':>6}  {'Supprimés':>9}")
    print("    " + "-" * 62)
    for src in all_sources:
        b = src_before.get(src, 0)
        a = src_after.get(src, 0)
        d = b - a
        print(f"    {src:<35} {b:>6,}  {a:>6,}  {d:>9,}")
    print()

    # Source distribution of rejections
    if not rejection_log.empty:
        print("  Suppressions par source :")
        for src, count in rejection_log["source"].value_counts().items():
            print(f"    {src:<35} {count:>6,}")
        print()

    print("=" * 60)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nettoie et normalise le corpus de headlines SectorPulse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        metavar="PATH",
        help=f"CSV d'entrée (défaut : {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        metavar="PATH",
        help=f"CSV nettoyé en sortie (défaut : {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG,
        metavar="PATH",
        help=f"Log CSV des articles supprimés (défaut : {DEFAULT_LOG}).",
    )
    parser.add_argument(
        "--min-title-len",
        type=int,
        default=DEFAULT_MIN_TITLE_LEN,
        metavar="INT",
        help=f"Longueur minimale du titre pour conserver l'article (défaut : {DEFAULT_MIN_TITLE_LEN}).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # --- Load ----------------------------------------------------------------
    if not args.input.exists():
        print(f"[ERROR] Fichier introuvable : {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"Chargement de {args.input} …")
    original = pd.read_csv(args.input, dtype=str)
    print(f"  {len(original):,} articles chargés.")

    # --- Clean ---------------------------------------------------------------
    print("Nettoyage en cours …")
    cleaned, rejection_log = clean(original, min_title_len=args.min_title_len)

    # --- Save ----------------------------------------------------------------
    args.output.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(args.output, index=False)
    print(f"  CSV nettoyé sauvegardé : {args.output}  ({len(cleaned):,} lignes)")

    args.log.parent.mkdir(parents=True, exist_ok=True)
    rejection_log.to_csv(args.log, index=False)
    print(f"  Log des rejets sauvegardé : {args.log}  ({len(rejection_log):,} lignes)")

    # --- Report --------------------------------------------------------------
    print_stats(original, cleaned, rejection_log)


if __name__ == "__main__":
    main()
