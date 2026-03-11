"""
Build pair_observations.csv by combining scraper outputs and optional baseline rows.
This step merges rows from multiple scraper outputs, picking the best row for each side by price presence, confidence, and match quality.
"""

from __future__ import annotations
from collections import defaultdict
from pathlib import Path
import argparse
import csv
import sys

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import default_data_date, get_paths
from pink_tax.scraping_utils.normalize import keyword_gender_label, normalize_gender
from pink_tax.utils import (
    backup_existing_file,
    format_number_str,
    normalize_confidence,
    parse_binary_flag,
    to_float,
)

paths = get_paths(root)

output_fields = [
    "pair_code",
    "city",
    "brand",
    "category",
    "female_product",
    "male_product",
    "female_size",
    "male_size",
    "retailer",
    "date_observed",
    "female_price_local",
    "male_price_local",
    "currency",
    "female_on_promo",
    "male_on_promo",
    "match_quality",
    "confidence",
    "match_notes",
]

confidence_rank = {"LOW": 1, "MED": 2, "HIGH": 3}
rank_to_confidence = {1: "LOW", 2: "MED", 3: "HIGH"}

def normalize_gender_from_row(row: dict) -> str:
    """
    Resolve gender classification from explicit labels, then keyword fallback.
    """

    for field in ("gender_label", "expected_gender_label"):
        value = normalize_gender(str(row.get(field, "")).strip())
        if value in {"female", "male"}:
            return value
        
    product_name = str(row.get("product_name", "")).strip()
    kw_label, _ = keyword_gender_label(product_name)

    if kw_label in {"female", "male"}:
        return kw_label
    
    return ""

def candidate_score(row: dict) -> tuple[int, int, float]:
    """
    Score candidate side rows for best-row selection.
    """

    price = to_float(row.get("price_local"))
    has_price = 1 if price is not None and price > 0 else 0
    conf = confidence_rank.get(normalize_confidence(row.get("confidence")), 1)
    mq = to_float(row.get("match_quality")) or 0.0

    return has_price, conf, mq


def pick_better_row(current: dict | None, candidate: dict) -> dict:
    """
    Pick the better row by price presence, confidence, and match quality.
    """

    if current is None:
        return candidate
    
    return candidate if candidate_score(candidate) > candidate_score(current) else current


def row_key(row: dict) -> tuple[str, str, str, str, str, str]:
    """
    Build grouping key for one scraped side row.
    """

    pair_code = str(row.get("pair_code") or row.get("pair_id") or "").strip()
    city = str(row.get("city", "")).strip()
    brand = str(row.get("brand", "")).strip()
    category = str(row.get("category", "")).strip()
    retailer = str(row.get("retailer", "")).strip()
    date_observed = str(row.get("date_observed") or row.get("date_scraped") or "").strip() or default_data_date

    return pair_code, city, brand, category, retailer, date_observed


def pair_map_from_scrape_csv(path: Path) -> tuple[dict[tuple, dict], int, int]:
    """
    Build pair rows from one scraper CSV.
    """

    groups: dict[tuple, dict[str, dict | None]] = defaultdict(lambda: {"female": None, "male": None})
    read_rows = 0

    for row in csv.DictReader(path.open(newline="", encoding="utf-8")):
        read_rows += 1
        key = row_key(row)

        if not key[0] or not key[1] or not key[4]:
            continue

        side = normalize_gender_from_row(row)

        if side not in {"female", "male"}:
            continue

        groups[key][side] = pick_better_row(groups[key][side], row)

    out: dict[tuple, dict] = {}

    for key, sides in groups.items():
        female = sides["female"]
        male = sides["male"]

        if female is None or male is None:
            continue

        female_price = to_float(female.get("price_local"))
        male_price = to_float(male.get("price_local"))
        female_size = to_float(female.get("size_ml_or_g") or female.get("female_size"))
        male_size = to_float(male.get("size_ml_or_g") or male.get("male_size"))

        if female_price is None or male_price is None or female_price <= 0 or male_price <= 0:
            continue
        if female_size is None or male_size is None or female_size <= 0 or male_size <= 0:
            continue

        pair_code, city, brand, category, retailer, date_observed = key
        female_conf = normalize_confidence(female.get("confidence"))
        male_conf = normalize_confidence(male.get("confidence"))
        conservative_rank = min(confidence_rank[female_conf], confidence_rank[male_conf])
        confidence = rank_to_confidence[conservative_rank]
        mq_f = int(round(to_float(female.get("match_quality")) or 1.0))
        mq_m = int(round(to_float(male.get("match_quality")) or 1.0))
        match_quality = max(1, min(5, min(mq_f, mq_m)))

        notes = [f"source={path.name}"]
        f_status = str(female.get("scrape_status", "")).strip()
        m_status = str(male.get("scrape_status", "")).strip()

        if f_status and f_status != "OK":
            notes.append(f"female_status={f_status}")

        if m_status and m_status != "OK":
            notes.append(f"male_status={m_status}")

        currency_f = str(female.get("currency", "")).strip()
        currency_m = str(male.get("currency", "")).strip()
        currency = currency_f or currency_m

        if currency_f and currency_m and currency_f != currency_m:
            notes.append(f"currency_conflict={currency_f}|{currency_m}")

        out_key = (pair_code, retailer, date_observed)
        out[out_key] = {
            "pair_code": pair_code,
            "city": city,
            "brand": brand,
            "category": category,
            "female_product": str(female.get("product_name", "")).strip(),
            "male_product": str(male.get("product_name", "")).strip(),
            "female_size": format_number_str(female_size),
            "male_size": format_number_str(male_size),
            "retailer": retailer,
            "date_observed": date_observed,
            "female_price_local": format_number_str(female_price),
            "male_price_local": format_number_str(male_price),
            "currency": currency,
            "female_on_promo": str(parse_binary_flag(female.get("on_promotion"))),
            "male_on_promo": str(parse_binary_flag(male.get("on_promotion"))),
            "match_quality": str(match_quality),
            "confidence": confidence,
            "match_notes": "; ".join(notes),
        }

    return out, read_rows, len(out)

def load_baseline(path: Path) -> dict[tuple, dict]:
    """
    Load existing pair observations keyed for merge.
    """

    if not path.exists():
        return {}
    
    rows = {}

    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row.get("pair_code", "").strip(), row.get("retailer", "").strip(), row.get("date_observed", "").strip())
            
            if not key[0] or not key[1] or not key[2]:
                continue

            rows[key] = {field: str(row.get(field, "")).strip() for field in output_fields}
    
    return rows

def write_rows(path: Path, rows: list[dict]) -> None:
    """
    Write final pair observations CSV.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = backup_existing_file(path)
    if backup_path is not None:
        print(f"Backup created: {backup_path}")

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(rows)

def main() -> None:
    """
    CLI entrypoint.
    """

    parser = argparse.ArgumentParser(description="Build data/raw/pair_observations.csv from scraper outputs.")
    parser.add_argument(
        "--output-csv",
        default=str(paths.pair_observations_spec),
        help="Target pair_observations CSV path.",
    )
    parser.add_argument(
        "--baseline-csv",
        default=str(paths.pair_observations_spec),
        help="Existing pair_observations CSV used as baseline for merge.",
    )
    parser.add_argument(
        "--scrape-csv",
        action="append",
        default=[],
        help="Scraper CSV path to ingest (repeatable).",
    )
    parser.add_argument(
        "--replace-only",
        action="store_true",
        help="Do not keep non-overlapping baseline rows.",
    )
    parser.add_argument(
        "--min-output-rows",
        type=int,
        default=50,
        help="Safety floor: abort write if merged output has fewer rows than this.",
    )
    args = parser.parse_args()

    default_scrapes = [
        paths.data_raw / "amazon_in_raw.csv",
        paths.data_raw / "bigbasket_raw.csv",
        paths.data_raw / "flipkart_raw.csv",
        paths.data_raw / "amazon_jp_raw.csv",
        paths.data_raw / "rakuten_jp_raw.csv",
        paths.data_raw / "matsumoto_raw.csv",
    ]
    scrape_paths = [Path(p) for p in args.scrape_csv] if args.scrape_csv else default_scrapes

    combined_scraped: dict[tuple, dict] = {}
    total_rows_read = 0
    total_pairs_built = 0
    used_files = 0
    for path in scrape_paths:
        if not path.exists():
            print(f"Skip missing scrape file: {path}")
            continue
        scraped_map, read_rows, built_pairs = pair_map_from_scrape_csv(path)
        total_rows_read += read_rows
        total_pairs_built += built_pairs
        used_files += 1
        combined_scraped.update(scraped_map)
        print(f"Ingested {path.name}: rows={read_rows}, pair_rows={built_pairs}")

    baseline = {}
    if not args.replace_only:
        baseline = load_baseline(Path(args.baseline_csv))
        print(f"Loaded baseline rows: {len(baseline)} from {args.baseline_csv}")

    merged = dict(baseline)
    merged.update(combined_scraped)
    final_rows = sorted(
        merged.values(),
        key=lambda r: (
            r["city"],
            r["pair_code"],
            r["retailer"],
            r["date_observed"],
        ),
    )

    if len(final_rows) < args.min_output_rows:
        raise SystemExit(
            "Refusing to overwrite pair_observations.csv because output is unexpectedly small: "
            f"{len(final_rows)} rows (< min-output-rows {args.min_output_rows}). "
            "Use --min-output-rows 0 to force write."
        )

    write_rows(Path(args.output_csv), final_rows)

if __name__ == "__main__":
    main()
