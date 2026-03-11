"""
Clean the generated pairs dataset.
This step removes duplicate rows, drops rows with missing critical values,
filters invalid numeric entries, and removes extreme outliers.
"""

from __future__ import annotations
from collections import defaultdict
from pathlib import Path
from typing import cast
import argparse
import csv
import sys

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import (
    default_clean_drop_from_column,
    default_clean_max_abs_pink_tax,
    default_clean_max_price_ratio,
    default_clean_max_size_ratio,
    default_clean_min_match_quality,
    default_clean_min_price_ratio,
    default_clean_min_size_ratio,
    get_paths,
)

from pink_tax.utils import backup_existing_file, is_blank, to_float

paths = get_paths(root)

required_fields = [
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
    "female_ppu_local",
    "male_ppu_local",
    "pink_tax_pct",
    "match_quality",
]

numeric_fields = [
    "female_price_local",
    "male_price_local",
    "female_size",
    "male_size",
    "female_ppu_local",
    "male_ppu_local",
    "pink_tax_pct",
    "match_quality",
]

dedupe_key_fields = [
    "pair_code",
    "city",
    "retailer",
    "date_observed",
    "female_product",
    "male_product",
]

confidence_rank = {"LOW": 1, "MED": 2, "HIGH": 3}

def dedupe_score(row: dict[str, str | None]) -> tuple[int, int, float, int]:
    """
    Score duplicate candidates and keep the best row.
    """

    completeness = sum(0 if is_blank(row.get(field)) else 1 for field in required_fields)
    confidence = str(row.get("confidence") or "").strip().upper()
    confidence_score = confidence_rank.get(confidence, 0)
    match_quality = to_float(row.get("match_quality")) or 0.0
    needs_review = str(row.get("needs_review") or "").strip().lower() in {"1", "true", "yes"}
    review_penalty = 0 if needs_review else 1

    return completeness, confidence_score, match_quality, review_penalty

def find_best_row(rows: list[dict[str, str | None]]) -> int:
    """
    Return index of best row by score.
    """

    best_idx = 0
    best_score = dedupe_score(rows[0])

    for idx in range(1, len(rows)):
        score = dedupe_score(rows[idx])
        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx

def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """
    Write rows to CSV.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = backup_existing_file(path)
    if backup_path is not None:
        print(f"Backup created: {backup_path}")

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def select_final_fieldnames(fieldnames: list[str], drop_from_column: str) -> list[str]:
    """
    Return output field list truncated from a named start column.
    """

    if drop_from_column in fieldnames:
        idx = fieldnames.index(drop_from_column)
        return fieldnames[:idx]
    
    return list(fieldnames)

def project_rows(rows: list[dict[str, str | None]], fieldnames: list[str]) -> list[dict]:
    """
    Project row dictionaries to the selected output fields.
    """

    projected: list[dict] = []

    for row in rows:
        projected.append({field: row.get(field, "") for field in fieldnames})

    return projected

def clean_dataset(
    input_csv: Path,
    output_csv: Path,
    max_abs_pink_tax: float,
    min_match_quality: int,
    min_size_ratio: float,
    max_size_ratio: float,
    min_price_ratio: float,
    max_price_ratio: float,
    drop_from_column: str,
) -> tuple[int, int, int]:
    """
    Clean dataset and return before, after, removed counts.
    """

    with input_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = cast(list[str], list(reader.fieldnames or []))
        rows = cast(list[dict[str, str | None]], list(reader))

    if not rows:
        raise ValueError("Input dataset is empty.")
    if not fieldnames:
        raise ValueError("Input dataset has no header row.")

    grouped: dict[tuple[str, ...], list[dict[str, str | None]]] = defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(field) or "").strip() for field in dedupe_key_fields)
        grouped[key].append(row)

    deduped_rows: list[dict[str, str | None]] = []
    removed_rows: list[dict[str, str | None]] = []

    for group_rows in grouped.values():
        if len(group_rows) == 1:
            deduped_rows.append(group_rows[0])
            continue
        best_idx = find_best_row(group_rows)
        for idx, row in enumerate(group_rows):
            if idx == best_idx:
                deduped_rows.append(row)
            else:
                dropped = dict(row)
                dropped["removed_reason"] = "duplicate_row"
                removed_rows.append(dropped)

    cleaned_rows: list[dict[str, str | None]] = []
    for row in deduped_rows:
        reasons: list[str] = []

        for field in required_fields:
            if is_blank(row.get(field)):
                reasons.append("missing_required_field")
                break

        parsed: dict[str, float] = {}
        if not reasons:
            for field in numeric_fields:
                value = to_float(row.get(field))
                if value is None:
                    reasons.append("invalid_numeric")
                    break
                parsed[field] = value

        if not reasons:
            if parsed["female_price_local"] <= 0 or parsed["male_price_local"] <= 0:
                reasons.append("nonpositive_price")
            if parsed["female_size"] <= 0 or parsed["male_size"] <= 0:
                reasons.append("nonpositive_size")
            if parsed["female_ppu_local"] <= 0 or parsed["male_ppu_local"] <= 0:
                reasons.append("nonpositive_ppu")

        if not reasons:
            if int(round(parsed["match_quality"])) < min_match_quality:
                reasons.append("low_match_quality")

            if abs(parsed["pink_tax_pct"]) > max_abs_pink_tax:
                reasons.append("extreme_pink_tax")

            size_ratio = parsed["female_size"] / parsed["male_size"]
            if size_ratio < min_size_ratio or size_ratio > max_size_ratio:
                reasons.append("extreme_size_ratio")

            price_ratio = parsed["female_price_local"] / parsed["male_price_local"]
            if price_ratio < min_price_ratio or price_ratio > max_price_ratio:
                reasons.append("extreme_price_ratio")

        if reasons:
            dropped = dict(row)
            dropped["removed_reason"] = "|".join(sorted(set(reasons)))
            removed_rows.append(dropped)
        else:
            cleaned_rows.append(row)

    final_fieldnames = select_final_fieldnames(fieldnames, drop_from_column)
    final_rows = project_rows(cleaned_rows, final_fieldnames)
    write_csv(output_csv, final_rows, final_fieldnames)

    total_rows = len(rows)
    kept_rows = len(cleaned_rows)
    removed_count = len(removed_rows)

    return total_rows, kept_rows, removed_count

def main() -> None:
    """
    CLI entrypoint.
    """

    parser = argparse.ArgumentParser(description="Clean duplicates, missing values, and outliers in pairs dataset.")
    parser.add_argument("--input-csv", default=str(paths.pairs_csv), help="Input pairs CSV.")
    parser.add_argument("--output-csv", default=str(paths.pairs_csv), help="Output cleaned pairs CSV.")
    parser.add_argument("--max-abs-pink-tax", type=float, default=default_clean_max_abs_pink_tax)
    parser.add_argument("--min-match-quality", type=int, default=default_clean_min_match_quality)
    parser.add_argument("--min-size-ratio", type=float, default=default_clean_min_size_ratio)
    parser.add_argument("--max-size-ratio", type=float, default=default_clean_max_size_ratio)
    parser.add_argument("--min-price-ratio", type=float, default=default_clean_min_price_ratio)
    parser.add_argument("--max-price-ratio", type=float, default=default_clean_max_price_ratio)
    parser.add_argument(
        "--drop-from-column",
        default=default_clean_drop_from_column,
        help="Drop this column and everything to its right in final dataset output.",
    )
    args = parser.parse_args()

    before, after, removed = clean_dataset(
        input_csv=Path(args.input_csv),
        output_csv=Path(args.output_csv),
        max_abs_pink_tax=args.max_abs_pink_tax,
        min_match_quality=args.min_match_quality,
        min_size_ratio=args.min_size_ratio,
        max_size_ratio=args.max_size_ratio,
        min_price_ratio=args.min_price_ratio,
        max_price_ratio=args.max_price_ratio,
        drop_from_column=args.drop_from_column,
    )
    print(f"Input rows: {before}")
    print(f"Cleaned rows: {after}")
    print(f"Removed rows: {removed}")
    print(f"Cleaned dataset: {args.output_csv}")

if __name__ == "__main__":
    main()
