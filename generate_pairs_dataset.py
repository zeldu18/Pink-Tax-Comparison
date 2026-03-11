"""
Generate the final cleaned dataset CSV from observation data with model-driven gender labels.
"""

from __future__ import annotations
from collections import Counter
from pathlib import Path
import argparse
import csv
import sys

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import default_model_name, default_model_threshold, get_paths
from pink_tax.scraping_utils.gender_labeler import ModelGenderLabeler
from pink_tax.scraping_utils import normalize as normalize_utils
from pink_tax.utils import backup_existing_file, parse_binary_flag

paths = get_paths(root)

input_fields = [
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

optional_input_fields = [
    "female_manual_override",
    "male_manual_override",
]

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
    "female_ppu_local",
    "male_ppu_local",
    "pink_tax_pct",
    "match_quality",
    "confidence",
    "match_notes",
    "gender_model_name",
    "gender_model_threshold",
    "female_expected_gender",
    "male_expected_gender",
    "female_keyword_gender",
    "male_keyword_gender",
    "female_keyword_evidence",
    "male_keyword_evidence",
    "female_model_gender",
    "male_model_gender",
    "female_model_confidence",
    "male_model_confidence",
    "female_manual_override",
    "male_manual_override",
    "female_final_gender",
    "male_final_gender",
    "female_label_source",
    "male_label_source",
    "female_needs_review",
    "male_needs_review",
    "needs_review",
]

allowed_gender_values = getattr(
    normalize_utils,
    "allowed_gender_values",
    getattr(normalize_utils, "ALLOWED_GENDER_VALUES", {"female", "male", "neutral"}),
)
allowed_genders = set(allowed_gender_values)

def parse_number(raw: str, name: str) -> float:
    """
    Parse numeric fields with explicit error context.
    """

    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid numeric value for '{name}': {raw!r}") from exc

def clean_scalar(value: float) -> int | float:
    """
    Normalize float values that are effectively integers.
    """

    rounded = round(value)
    if abs(value - rounded) < 1e-9:
        return int(rounded)
    return value

def compute_ppu(price_local: float, size_value: float) -> float:
    """
    Compute price per unit with the same legacy rule.
    """

    if size_value <= 0 or size_value == 1:
        return float(price_local)
    return round(price_local / size_value, 6)

def compute_pink_tax_pct(female_ppu: float, male_ppu: float) -> float:
    """
    Compute pink tax percentage from per-unit prices.
    """

    if male_ppu == 0:
        return 0.0
    return round((female_ppu - male_ppu) / male_ppu * 100, 4)

def parse_manual_override(raw: str) -> str:
    """
    Normalize manual override values to female/male/neutral or blank.
    """

    text = str(raw or "").strip()

    if not text:
        return ""
    
    normalized = normalize_utils.normalize_gender(text)

    if normalized in allowed_genders:
        return normalized
    return ""

def classify_product(
    product_name: str,
    expected: str,
    manual_override: str,
    classifier: ModelGenderLabeler,
) -> dict:
    """
    Return complete label metadata for one product string.
    """

    predicted = classifier.classify(
        product_name=product_name,
        expected_label=expected,
        manual_override=manual_override,
    )

    return {
        "expected_gender": predicted["expected_gender_label"],
        "keyword_gender": predicted["keyword_gender_label"],
        "keyword_evidence": predicted["keyword_evidence"],
        "model_gender": predicted["model_gender_label"],
        "model_confidence": predicted["model_gender_confidence"],
        "manual_override": manual_override,
        "final_gender": predicted["gender_label"],
        "label_source": predicted["gender_label_source"],
        "needs_review": predicted["gender_needs_review"],
    }

def load_spec_rows(spec_csv: Path) -> list[dict]:
    """
    Load and validate source observation rows.
    """

    with spec_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("Spec CSV has no header row.")
        missing = [col for col in input_fields if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"Spec CSV is missing required columns: {missing}")

        rows = list(reader)
        for row in rows:
            for col in optional_input_fields:
                row.setdefault(col, "")
        return rows

def build_output_rows(
    spec_rows: list[dict],
    classifier: ModelGenderLabeler,
) -> list[dict]:
    """
    Build output rows and compute derived metrics.
    """

    output: list[dict] = []

    for idx, row in enumerate(spec_rows, start=1):
        female_size = parse_number(row["female_size"], "female_size")
        male_size = parse_number(row["male_size"], "male_size")
        female_price = parse_number(row["female_price_local"], "female_price_local")
        male_price = parse_number(row["male_price_local"], "male_price_local")
        match_quality = int(parse_number(row["match_quality"], "match_quality"))

        if match_quality < 1 or match_quality > 5:
            raise ValueError(f"Row {idx}: match_quality out of range [1,5].")

        female_ppu = compute_ppu(female_price, female_size)
        male_ppu = compute_ppu(male_price, male_size)
        pink_tax_pct = compute_pink_tax_pct(female_ppu, male_ppu)

        female_manual = parse_manual_override(row.get("female_manual_override", ""))
        male_manual = parse_manual_override(row.get("male_manual_override", ""))

        female_label = classify_product(
            product_name=row["female_product"],
            expected="female",
            manual_override=female_manual,
            classifier=classifier,
        )
        male_label = classify_product(
            product_name=row["male_product"],
            expected="male",
            manual_override=male_manual,
            classifier=classifier,
        )

        output.append(
            {
                "pair_code": row["pair_code"].strip(),
                "city": row["city"].strip(),
                "brand": row["brand"].strip(),
                "category": row["category"].strip(),
                "female_product": row["female_product"].strip(),
                "male_product": row["male_product"].strip(),
                "female_size": clean_scalar(female_size),
                "male_size": clean_scalar(male_size),
                "retailer": row["retailer"].strip(),
                "date_observed": row["date_observed"].strip(),
                "female_price_local": clean_scalar(female_price),
                "male_price_local": clean_scalar(male_price),
                "currency": row["currency"].strip(),
                "female_on_promo": parse_binary_flag(row["female_on_promo"]),
                "male_on_promo": parse_binary_flag(row["male_on_promo"]),
                "female_ppu_local": female_ppu,
                "male_ppu_local": male_ppu,
                "pink_tax_pct": pink_tax_pct,
                "match_quality": match_quality,
                "confidence": row["confidence"].strip().upper(),
                "match_notes": row.get("match_notes", "").strip(),
                "gender_model_name": classifier.model_name,
                "gender_model_threshold": round(classifier.threshold, 4),
                "female_expected_gender": female_label["expected_gender"],
                "male_expected_gender": male_label["expected_gender"],
                "female_keyword_gender": female_label["keyword_gender"],
                "male_keyword_gender": male_label["keyword_gender"],
                "female_keyword_evidence": female_label["keyword_evidence"],
                "male_keyword_evidence": male_label["keyword_evidence"],
                "female_model_gender": female_label["model_gender"],
                "male_model_gender": male_label["model_gender"],
                "female_model_confidence": female_label["model_confidence"],
                "male_model_confidence": male_label["model_confidence"],
                "female_manual_override": female_label["manual_override"],
                "male_manual_override": male_label["manual_override"],
                "female_final_gender": female_label["final_gender"],
                "male_final_gender": male_label["final_gender"],
                "female_label_source": female_label["label_source"],
                "male_label_source": male_label["label_source"],
                "female_needs_review": female_label["needs_review"],
                "male_needs_review": male_label["needs_review"],
                "needs_review": int(
                    bool(female_label["needs_review"]) or bool(male_label["needs_review"])
                ),
            }
        )
    return output

def write_output_csv(output_csv: Path, rows: list[dict]) -> None:
    """
    Write generated dataset rows to CSV.
    """

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    backup_path = backup_existing_file(output_csv)
    if backup_path is not None:
        print(f"Backup created: {backup_path}")

    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(rows)

def print_summary(rows: list[dict]) -> None:
    """
    Print generation and label-quality summary stats.
    """

    observation_keys = {
        (r["pair_code"], r["retailer"], r["date_observed"])
        for r in rows
    }
    hyd_observations = {
        (r["pair_code"], r["retailer"], r["date_observed"])
        for r in rows
        if r["city"] == "Hyderabad"
    }
    tky_observations = {
        (r["pair_code"], r["retailer"], r["date_observed"])
        for r in rows
        if r["city"] == "Tokyo"
    }
    retailers = Counter(r["retailer"] for r in rows)
    categories = Counter(r["category"] for r in rows)
    positive = sum(1 for r in rows if r["pink_tax_pct"] > 0 and r["confidence"] != "LOW")
    negative = sum(1 for r in rows if r["pink_tax_pct"] < 0)
    review_rows = sum(1 for r in rows if int(r["needs_review"]) == 1)

    positive_vals = [
        r["pink_tax_pct"] for r in rows if r["pink_tax_pct"] > 0 and r["confidence"] != "LOW"
    ]
    avg_positive = sum(positive_vals) / len(positive_vals) if positive_vals else 0.0

    female_sources = Counter(r["female_label_source"] for r in rows)
    male_sources = Counter(r["male_label_source"] for r in rows)

    print(f"Total rows: {len(rows)}")
    print(f"Unique observations: {len(observation_keys)}")
    print(f"  Hyderabad: {len(hyd_observations)}")
    print(f"  Tokyo:     {len(tky_observations)}")
    print("\nRetailers:")
    for retailer, count in sorted(retailers.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {retailer:<35} {count} rows")
    print("\nCategories:")
    for category, count in sorted(categories.items(), key=lambda item: -item[1]):
        print(f"  {category:<35} {count} rows")

    print(f"\nPink tax > 0: {positive} rows")
    print(f"Pink tax < 0: {negative} rows (blue tax / negative)")
    print(f"Average pink tax (positive rows): {avg_positive:.1f}%")

    print("\nGender labeling quality (hybrid model+keyword):")
    print(f"  Rows flagged for review: {review_rows}/{len(rows)}")
    print("  Female label sources:")
    for source, count in sorted(female_sources.items(), key=lambda x: (-x[1], x[0])):
        print(f"    {source:<24} {count}")
    print("  Male label sources:")
    for source, count in sorted(male_sources.items(), key=lambda x: (-x[1], x[0])):
        print(f"    {source:<24} {count}")

def main() -> None:
    """
    CLI entrypoint.
    """

    parser = argparse.ArgumentParser(
        description="Generate pink tax pairs CSV from observation spec with model-driven labels."
    )
    parser.add_argument(
        "--spec-csv",
        default=str(paths.pair_observations_spec),
        help="Input observation spec CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default=str(paths.pairs_csv),
        help="Output generated pairs CSV.",
    )
    parser.add_argument(
        "--model-name",
        default=default_model_name,
        help="Hugging Face model name for zero-shot gender classification.",
    )
    parser.add_argument(
        "--model-threshold",
        default=default_model_threshold,
        type=float,
        help="Minimum confidence required before an item avoids review flagging.",
    )
    parser.add_argument(
        "--model-cache",
        default=str(paths.data_raw / "gender_model_cache.json"),
        help="Persistent JSON cache for model predictions.",
    )
    args = parser.parse_args()

    spec_csv = Path(args.spec_csv)
    output_csv = Path(args.output_csv)
    cache_path = Path(args.model_cache)

    classifier = ModelGenderLabeler(
        model_name=args.model_name,
        cache_path=cache_path,
        threshold=args.model_threshold,
    )

    rows = build_output_rows(
        spec_rows=load_spec_rows(spec_csv),
        classifier=classifier,
    )
    write_output_csv(output_csv, rows)
    classifier.persist()
    print_summary(rows)

if __name__ == "__main__":
    main()
