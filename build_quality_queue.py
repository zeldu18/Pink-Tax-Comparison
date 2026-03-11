"""
Build a quality summary from the cleaned pairs dataset.
This script scans the cleaned pairs CSV and applies validation rules, then writes only a summary.
"""

from __future__ import annotations
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
import argparse
import csv
import sys
from typing import cast

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import (
    default_quality_min_category_pairs,
    default_quality_min_city_pairs,
    default_quality_min_match_quality,
    default_quality_pink_tax_abs_threshold,
    default_quality_recommended_city_pairs,
    get_paths,
)
from pink_tax.utils import parse_date_yyyy_mm_dd, to_float

paths = get_paths(root)

core_fields = [
    "pair_code",
    "city",
    "brand",
    "category",
    "female_product",
    "male_product",
    "retailer",
    "date_observed",
    "currency",
    "female_price_local",
    "male_price_local",
    "female_size",
    "male_size",
    "pink_tax_pct",
    "match_quality",
]

valid_cities = {"Hyderabad", "Tokyo"}
valid_confidence = {"HIGH", "MED", "LOW"}
expected_city_currency = {"Hyderabad": "INR", "Tokyo": "JPY"}
expected_retailers_by_city = {
    "Hyderabad": {"Amazon.in", "BigBasket", "Flipkart"},
    "Tokyo": {"Amazon.co.jp", "Matsumoto Kiyoshi", "Rakuten Japan"},
}

issue_severity = {
    "missing_core_field": "high",
    "invalid_numeric": "high",
    "nonpositive_price": "high",
    "nonpositive_size": "high",
    "invalid_match_quality": "high",
    "city_currency_mismatch": "high",
    "invalid_retailer_for_city": "high",
    "female_side_label_mismatch": "high",
    "male_side_label_mismatch": "high",
    "duplicate_row": "high",
    "invalid_city": "high",
    "invalid_date_format": "high",
    "future_date_observed": "high",
    "pair_code_city_token_mismatch": "high",
    "low_match_quality": "medium",
    "extreme_pink_tax": "medium",
    "gender_needs_review": "medium",
    "city_pair_count_below_min": "medium",
    "city_category_pair_count_low": "medium",
    "suspicious_size_ratio": "medium",
    "suspicious_price_ratio": "medium",
    "unknown_confidence_value": "medium",
    "duplicate_product_name": "medium",
    "city_pair_count_below_recommended": "low",
    "manual_override_used": "low",
}

def row_issues(
    row: dict[str, str | None], pink_tax_abs_threshold: float, min_quality: int
) -> list[str]:
    """
    Return issue codes for one cleaned row.
    """

    issues: list[str] = []

    for field in core_fields:
        if str(row.get(field, "")).strip() == "":
            issues.append("missing_core_field")
            break

    female_price = to_float(row.get("female_price_local"))
    male_price = to_float(row.get("male_price_local"))
    female_size = to_float(row.get("female_size"))
    male_size = to_float(row.get("male_size"))
    pink_tax = to_float(row.get("pink_tax_pct"))
    quality = to_float(row.get("match_quality"))

    if female_price is None or male_price is None or female_size is None or male_size is None or pink_tax is None:
        issues.append("invalid_numeric")
    else:
        if female_price <= 0 or male_price <= 0:
            issues.append("nonpositive_price")
        if female_size <= 0 or male_size <= 0:
            issues.append("nonpositive_size")
        if abs(pink_tax) > pink_tax_abs_threshold:
            issues.append("extreme_pink_tax")

    if quality is None:
        issues.append("invalid_match_quality")
    elif quality < min_quality:
        issues.append("low_match_quality")

    city = str(row.get("city", "")).strip()
    if city not in valid_cities:
        issues.append("invalid_city")

    expected_currency = expected_city_currency.get(city)
    if expected_currency and row.get("currency") != expected_currency:
        issues.append("city_currency_mismatch")

    retailer = str(row.get("retailer", "")).strip()
    expected_retailers = expected_retailers_by_city.get(city)
    if expected_retailers and retailer and retailer not in expected_retailers:
        issues.append("invalid_retailer_for_city")

    observed_date = parse_date_yyyy_mm_dd(row.get("date_observed"))
    if observed_date is None:
        issues.append("invalid_date_format")
    elif observed_date > date.today():
        issues.append("future_date_observed")

    confidence = str(row.get("confidence", "")).strip().upper()
    if confidence and confidence not in valid_confidence:
        issues.append("unknown_confidence_value")

    if str(row.get("needs_review", "0")).strip() in {"1", "true", "True", "yes"}:
        issues.append("gender_needs_review")

    female_final = str(row.get("female_final_gender", "")).strip()
    male_final = str(row.get("male_final_gender", "")).strip()
    if female_final and female_final != "female":
        issues.append("female_side_label_mismatch")
    if male_final and male_final != "male":
        issues.append("male_side_label_mismatch")

    if str(row.get("female_manual_override", "")).strip() or str(
        row.get("male_manual_override", "")
    ).strip():
        issues.append("manual_override_used")

    pair_code = str(row.get("pair_code", "")).upper()
    if "HYD" in pair_code and city and city != "Hyderabad":
        issues.append("pair_code_city_token_mismatch")
    if "TKY" in pair_code and city and city != "Tokyo":
        issues.append("pair_code_city_token_mismatch")

    female_product = str(row.get("female_product", "")).strip().lower()
    male_product = str(row.get("male_product", "")).strip().lower()
    if female_product and male_product and female_product == male_product:
        issues.append("duplicate_product_name")

    if female_size and male_size and female_size > 0 and male_size > 0:
        size_ratio = female_size / male_size
        if size_ratio > 3 or size_ratio < (1 / 3):
            issues.append("suspicious_size_ratio")

    if female_price and male_price and female_price > 0 and male_price > 0:
        price_ratio = female_price / male_price
        if price_ratio > 5 or price_ratio < 0.2:
            issues.append("suspicious_price_ratio")

    return sorted(set(issues))

def row_severity(issues: list[str]) -> str:
    """
    Return highest severity level among issue codes.
    """

    if not issues:
        return "none"
    
    order = {"high": 3, "medium": 2, "low": 1, "none": 0}
    top = "low"

    for issue in issues:
        level = issue_severity.get(issue, "medium")
        if order[level] > order[top]:
            top = level

    return top

def duplicate_issue_map(rows: list[dict[str, str | None]]) -> dict[int, bool]:
    """
    Mark duplicate rows by a strict row identity key.
    """

    key_to_idx: dict[tuple, list[int]] = defaultdict(list)

    for idx, row in enumerate(rows):
        key = (
            row.get("pair_code", ""),
            row.get("city", ""),
            row.get("retailer", ""),
            row.get("date_observed", ""),
            row.get("female_product", ""),
            row.get("male_product", ""),
        )
        key_to_idx[key].append(idx)

    dup_map: dict[int, bool] = {}

    for idxs in key_to_idx.values():
        is_dup = len(idxs) > 1
        for idx in idxs:
            dup_map[idx] = is_dup

    return dup_map

def city_category_counts(
    rows: list[dict[str, str | None]]
) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    """
    Count unique observation keys per city and per (city, category).
    """

    city_observations: dict[str, set] = defaultdict(set)
    city_cat_observations: dict[tuple[str, str], set] = defaultdict(set)

    for row in rows:
        city = str(row.get("city") or "").strip()
        cat = str(row.get("category") or "").strip()
        pair = str(row.get("pair_code") or "").strip()
        retailer = str(row.get("retailer") or "").strip()
        observed = str(row.get("date_observed") or "").strip()

        observation_key = (pair, retailer, observed)

        if city and pair and retailer and observed:
            city_observations[city].add(observation_key)
        if city and cat and pair and retailer and observed:
            city_cat_observations[(city, cat)].add(observation_key)

    city_counts = {city: len(observations) for city, observations in city_observations.items()}
    city_cat_counts = {
        (city, cat): len(observations)
        for (city, cat), observations in city_cat_observations.items()
    }

    return city_counts, city_cat_counts

def retailer_counts(rows: list[dict[str, str | None]]) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    """
    Count row totals by retailer and by (city, retailer).
    """

    by_retailer: Counter = Counter()
    by_city_retailer: Counter = Counter()

    for row in rows:
        city = str(row.get("city") or "").strip()
        retailer = str(row.get("retailer") or "").strip()
        if not retailer:
            continue
        by_retailer[retailer] += 1
        if city:
            by_city_retailer[(city, retailer)] += 1

    return dict(by_retailer), dict(by_city_retailer)

def build_quality_queue(
    input_csv: Path,
    summary_csv: Path,
    min_city_pairs: int,
    recommended_city_pairs: int,
    min_category_pairs: int,
    pink_tax_abs_threshold: float,
    min_quality: int,
) -> tuple[int, int]:
    """
    Generate quality summary from cleaned pairs CSV.
    """

    with input_csv.open(newline="", encoding="utf-8") as handle:
        rows = cast(list[dict[str, str | None]], list(csv.DictReader(handle)))

    if not rows:
        raise ValueError("Input cleaned dataset is empty.")

    duplicate_map = duplicate_issue_map(rows)
    city_counts, city_cat_counts = city_category_counts(rows)
    retailer_row_counts, city_retailer_row_counts = retailer_counts(rows)

    flagged_count = 0
    issue_counter: Counter = Counter()
    severity_counter: Counter = Counter()

    for idx, row in enumerate(rows):
        issues = row_issues(row, pink_tax_abs_threshold=pink_tax_abs_threshold, min_quality=min_quality)

        if duplicate_map.get(idx, False):
            issues.append("duplicate_row")

        city = str(row.get("city") or "").strip()
        cat = str(row.get("category") or "").strip()

        city_n = city_counts.get(city, 0)
        if city_n < min_city_pairs:
            issues.append("city_pair_count_below_min")
        elif city_n < recommended_city_pairs:
            issues.append("city_pair_count_below_recommended")

        city_cat_n = city_cat_counts.get((city, cat), 0)
        if city_cat_n < min_category_pairs:
            issues.append("city_category_pair_count_low")

        issues = sorted(set(issues))
        if issues:
            issue_counter.update(issues)
            severity = row_severity(issues)
            severity_counter[severity] += 1
            flagged_count += 1

    summary_csv.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = [
        {"metric": "total_rows", "value": str(len(rows))},
        {"metric": "flagged_rows", "value": str(flagged_count)},
        {"metric": "flagged_pct", "value": f"{(flagged_count/len(rows))*100:.2f}"},
    ]
    for severity in ("high", "medium", "low"):
        summary_rows.append(
            {"metric": f"severity:{severity}", "value": str(severity_counter.get(severity, 0))}
        )
    for issue, count in sorted(issue_counter.items(), key=lambda x: (-x[1], x[0])):
        summary_rows.append({"metric": f"issue:{issue}", "value": str(count)})
    for city, count in sorted(city_counts.items()):
        summary_rows.append({"metric": f"city_unique_pairs:{city}", "value": str(count)})
    for retailer, count in sorted(retailer_row_counts.items()):
        summary_rows.append({"metric": f"retailer_rows:{retailer}", "value": str(count)})
    for (city, retailer), count in sorted(city_retailer_row_counts.items()):
        summary_rows.append({"metric": f"city_retailer_rows:{city}:{retailer}", "value": str(count)})

    with summary_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerows(summary_rows)

    return len(rows), flagged_count

def main() -> None:
    """
    CLI entrypoint.
    """

    parser = argparse.ArgumentParser(description="Build quality summary for cleaned pairs dataset.")
    parser.add_argument("--input-csv", default=str(paths.pairs_csv), help="Input cleaned pairs CSV.")
    parser.add_argument(
        "--summary-csv",
        default=str(paths.data_clean / "pink_tax_quality_review_summary.csv"),
        help="Output summary CSV with issue counts and coverage metrics.",
    )
    parser.add_argument("--min-city-pairs", type=int, default=default_quality_min_city_pairs)
    parser.add_argument(
        "--recommended-city-pairs",
        type=int,
        default=default_quality_recommended_city_pairs,
    )
    parser.add_argument("--min-category-pairs", type=int, default=default_quality_min_category_pairs)
    parser.add_argument(
        "--pink-tax-abs-threshold",
        type=float,
        default=default_quality_pink_tax_abs_threshold,
    )
    parser.add_argument("--min-quality", type=int, default=default_quality_min_match_quality)
    args = parser.parse_args()

    total, flagged = build_quality_queue(
        input_csv=Path(args.input_csv),
        summary_csv=Path(args.summary_csv),
        min_city_pairs=args.min_city_pairs,
        recommended_city_pairs=args.recommended_city_pairs,
        min_category_pairs=args.min_category_pairs,
        pink_tax_abs_threshold=args.pink_tax_abs_threshold,
        min_quality=args.min_quality,
    )
    print(f"Total rows scanned: {total}")
    print(f"Rows flagged for later cleaning: {flagged}")
    print(f"Quality summary: {args.summary_csv}")

if __name__ == "__main__":
    main()
