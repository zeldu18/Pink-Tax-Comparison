"""
Build large real scraper target lists from Open Beauty cache keys.
"""

from __future__ import annotations
from collections import defaultdict
from pathlib import Path
import json
import re

key_pattern = re.compile(r"^(.*?)::(.*?)::(female|male|neutral)::(.*)$", re.IGNORECASE)
size_pattern = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|g|gram|grams|gm|kg|l)\b", re.IGNORECASE)

def city_code(city: str) -> str:
    """
    Return compact city code used in generated pair ids.
    """

    normalized = str(city or "").strip().lower()
    if normalized == "hyderabad":
        return "HYD"
    if normalized == "tokyo":
        return "TKY"
    return re.sub(r"[^A-Z]", "", str(city or "").upper())[:3] or "CTY"

def slug_token(text: str, max_len: int) -> str:
    """
    Return uppercase alphanumeric slug token.
    """

    cleaned = re.sub(r"[^A-Z0-9]", "", str(text or "").upper())
    return (cleaned[:max_len] or "X")

def parse_size_from_name(product_name: str) -> float:
    """
    Parse rough size from product title with a conservative fallback.
    """

    text = str(product_name or "")
    match = size_pattern.search(text)
    if not match:
        return 100.0

    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit == "kg":
        return value * 1000.0
    if unit == "l":
        return value * 1000.0
    return value

def gender_hint(gender: str, locale: str) -> str:
    """
    Return locale-aware query hint tokens.
    """

    g = str(gender or "").strip().lower()
    loc = str(locale or "").strip().lower()

    if loc == "jp":
        return "女性用 レディース" if g == "female" else "男性用 メンズ"
    return "for women ladies female" if g == "female" else "for men mens male"

def build_search_query(product_name: str, gender: str, locale: str) -> str:
    """
    Build search phrase for marketplace query.
    """

    hint = gender_hint(gender, locale)
    return f"{str(product_name or '').strip()} {hint}".strip()

def merge_target_products(base_targets: list[dict], extra_targets: list[dict]) -> list[dict]:
    """
    Merge target products and deduplicate by pair_id + gender + product_name.
    """

    merged = [dict(item) for item in base_targets]
    seen = {
        (
            str(item.get("pair_id", "")).strip(),
            str(item.get("gender_label", "")).strip().lower(),
            str(item.get("product_name", "")).strip().lower(),
        )
        for item in merged
    }

    for item in extra_targets:
        key = (
            str(item.get("pair_id", "")).strip(),
            str(item.get("gender_label", "")).strip().lower(),
            str(item.get("product_name", "")).strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(dict(item))

    return merged

def build_targets_from_obf_cache(
    obf_cache_path: Path,
    city: str,
    locale: str,
    max_pairs: int,
    min_match_quality: int = 3,
) -> list[dict]:
    """
    Build female/male matched targets from OBF cache key metadata.
    """

    if max_pairs <= 0 or not obf_cache_path.exists():
        return []

    try:
        payload = json.loads(obf_cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    if not isinstance(payload, dict):
        return []

    grouped: dict[tuple[str, str], dict[str, list[dict]]] = defaultdict(
        lambda: {"female": [], "male": []}
    )

    for key, value in payload.items():
        match = key_pattern.match(str(key))
        if not match:
            continue

        brand, category, gender, product_name = [token.strip() for token in match.groups()]
        gender_norm = gender.lower()
        if gender_norm not in {"female", "male"}:
            continue

        ingredients = ""
        if isinstance(value, dict):
            ingredients = str(value.get("ingredients_text", "")).strip()

        grouped[(brand, category)][gender_norm].append(
            {
                "brand": brand,
                "category": category,
                "gender_label": gender_norm,
                "product_name": product_name,
                "size_ml_or_g": parse_size_from_name(product_name),
                "ingredients": ingredients,
            }
        )

    out: list[dict] = []
    pair_index = 1
    city_short = city_code(city)

    for (brand, category), sides in sorted(grouped.items()):
        female_items = sides["female"]
        male_items = sides["male"]
        if not female_items or not male_items:
            continue

        pair_count = min(len(female_items), len(male_items))
        for idx in range(pair_count):
            female_item = female_items[idx]
            male_item = male_items[idx]
            pair_id = (
                f"{slug_token(brand, 6)}-{slug_token(category, 8)}-{city_short}-"
                f"AUTO{pair_index:03d}"
            )

            out.append(
                {
                    "pair_id": pair_id,
                    "gender_label": "female",
                    "product_name": female_item["product_name"],
                    "brand": brand,
                    "category": category,
                    "size_ml_or_g": female_item["size_ml_or_g"],
                    "match_quality": min_match_quality,
                    "ingredients": female_item["ingredients"],
                    "search_query": build_search_query(
                        female_item["product_name"], "female", locale
                    ),
                    "url": None,
                }
            )
            out.append(
                {
                    "pair_id": pair_id,
                    "gender_label": "male",
                    "product_name": male_item["product_name"],
                    "brand": brand,
                    "category": category,
                    "size_ml_or_g": male_item["size_ml_or_g"],
                    "match_quality": min_match_quality,
                    "ingredients": male_item["ingredients"],
                    "search_query": build_search_query(
                        male_item["product_name"], "male", locale
                    ),
                    "url": None,
                }
            )

            pair_index += 1
            if pair_index > max_pairs:
                return out

    return out