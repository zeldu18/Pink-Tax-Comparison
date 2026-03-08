"""
Load scraper-specific configuration from JSON files.
"""

from __future__ import annotations
from pathlib import Path
import json
import os


def _env_key(source_name: str) -> str:
    """
    Build env var key for per-source config override.
    """

    return f"PINK_TAX_SCRAPER_CONFIG_{source_name.upper()}"


def load_scraping_source_config(root: Path, source_name: str) -> dict:
    """
    Load one scraping source config JSON, with optional env override path.
    """

    override = os.getenv(_env_key(source_name), "").strip()
    config_path = Path(override) if override else root / "config" / "scraping" / f"{source_name}.json"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Missing scraper config for '{source_name}': {config_path}. "
            "Add the file or set env override."
        )

    with config_path.open(encoding="utf-8") as handle:
        return json.load(handle)
