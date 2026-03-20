"""
Shared utility helpers used across pipeline scripts.
"""

from __future__ import annotations
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

def to_float(raw: object | None) -> float | None:
    """
    Parse a value to float, returning None for blank or invalid values.
    """

    if raw is None:
        return None

    text = str(raw).strip()
    if not text:
        return None

    try:
        return float(text)
    except (TypeError, ValueError):
        return None

def is_blank(raw: object | None) -> bool:
    """
    Return True when a value is empty after trim.
    """

    return str(raw or "").strip() == ""

def parse_binary_flag(raw: object | None) -> int:
    """
    Normalize truthy flag values to 1 and all others to 0.
    """

    return 1 if str(raw or "").strip().lower() in {"1", "true", "yes", "y"} else 0

def normalize_confidence(
    raw: object | None,
    allowed: set[str] | None = None,
    fallback: str = "LOW",
) -> str:
    """
    Normalize confidence labels to a controlled set with fallback.
    """

    normalized_allowed = allowed or {"LOW", "MED", "HIGH"}
    text = str(raw or "").strip().upper()
    return text if text in normalized_allowed else fallback

def format_number_str(value: float) -> str:
    """
    Format numeric values without trailing .0 for integer-like values.
    """

    rounded = round(value)
    if abs(value - rounded) < 1e-9:
        return str(int(rounded))
    return f"{value:.6f}".rstrip("0").rstrip(".")

def parse_date_yyyy_mm_dd(raw: object | None) -> date | None:
    """
    Parse YYYY-MM-DD date safely.
    """

    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None

def backup_existing_file(path: Path, backup_dir_name: str = "_backups") -> Path | None:
    """
    Create a timestamped backup copy when the target file already exists.
    """

    if not path.exists() or not path.is_file():
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = path.parent / backup_dir_name
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.stem}_{timestamp}{path.suffix}"
    backup_path.write_bytes(path.read_bytes())
    return backup_path

def select_diverse_pair_codes(products: list[dict], limit: int) -> set[str]:
    """
    Select pair codes with balanced brand/category coverage for limited runs.
    """

    if limit <= 0:
        return set()

    pair_meta: list[tuple[str, str, str, int]] = []
    seen_codes: set[str] = set()
    for index, product in enumerate(products):
        pair_code = str(product.get("pair_code", "")).strip()
        if not pair_code or pair_code in seen_codes:
            continue
        seen_codes.add(pair_code)
        brand = str(product.get("brand", "")).strip()
        category = str(product.get("category", "")).strip()
        pair_meta.append((pair_code, brand, category, index))

    if limit >= len(pair_meta):
        return {code for code, _, _, _ in pair_meta}

    selected: list[str] = []
    selected_set: set[str] = set()
    brand_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()

    while len(selected) < limit:
        candidates = [meta for meta in pair_meta if meta[0] not in selected_set]
        if not candidates:
            break

        pair_code, brand, category, _ = min(
            candidates,
            key=lambda item: (
                brand_counts[item[1]],
                category_counts[item[2]],
                item[3],
            ),
        )
        selected.append(pair_code)
        selected_set.add(pair_code)
        brand_counts[brand] += 1
        category_counts[category] += 1

    return set(selected)


def enforce_single_window(driver: Any) -> None:
    """
    Keep Selenium bound to one primary window and close unexpected extra tabs.
    """

    try:
        handles = list(driver.window_handles)
    except Exception:
        return

    if not handles:
        return

    primary = handles[0]
    for handle in handles[1:]:
        try:
            driver.switch_to.window(handle)
            driver.close()
        except Exception:
            continue

    try:
        driver.switch_to.window(primary)
    except Exception:
        pass
