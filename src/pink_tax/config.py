"""
Central configuration helpers used across scripts.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
import os

def load_dotenv(dotenv_path: Path | None = None) -> None:
    """
    Load a local .env file into process environment if keys are not already set.
    """

    root = Path(__file__).resolve().parents[2]
    env_path = dotenv_path or (root / ".env")

    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')

        if key and key not in os.environ:
            os.environ[key] = value

load_dotenv()

def project_root() -> Path:
    """
    Return the repository root, optionally overridden by env.
    """

    env_root = os.getenv("PINK_TAX_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[2]

@dataclass(frozen=True)
class ProjectPaths:
    """
    Resolved project paths.
    """

    root: Path
    config_dir: Path
    data_dir: Path
    data_raw: Path
    data_clean: Path
    data_db: Path
    data_eda: Path
    data_analysis: Path
    data_logs: Path
    pairs_csv: Path
    obf_cache: Path
    ppp_rates: Path
    pair_observations_spec: Path
    pipeline_steps: Path

def get_paths(root: Path | None = None) -> ProjectPaths:
    """
    Build absolute paths for the current repository.
    """

    resolved_root = (root or project_root()).resolve()
    data_dir = resolved_root / "data"

    return ProjectPaths(
        root=resolved_root,
        config_dir=resolved_root / "config",
        data_dir=data_dir,
        data_raw=data_dir / "raw",
        data_clean=data_dir / "clean",
        data_db=data_dir / "db",
        data_eda=data_dir / "eda",
        data_analysis=data_dir / "analysis",
        data_logs=data_dir / "logs",
        pairs_csv=data_dir / "clean" / "pink_tax_final_dataset_cleaned.csv",
        obf_cache=data_dir / "raw" / "obf_cache.json",
        ppp_rates=data_dir / "raw" / "ppp_rates.json",
        pair_observations_spec=data_dir / "raw" / "pair_observations.csv",
        pipeline_steps=resolved_root / "config" / "pipeline_steps.json",
    )

def env_str(name: str, default: str) -> str:
    """
    Read a string env var with fallback.
    """

    return os.getenv(name, default)

def env_float(name: str, default: float) -> float:
    """
    Read a float env var with fallback.
    """
    
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default

def env_int(name: str, default: int) -> int:
    """
    Read an int env var with fallback.
    """

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

default_date = env_str("PINK_TAX_DATA_DATE", "2025-03-05")
default_sleep_sec = env_float("PINK_TAX_OBF_SLEEP_SECONDS", 1.2)
default_obf_search_url = env_str("PINK_TAX_OBF_SEARCH_URL", "https://world.openbeautyfacts.org/cgi/search.pl")
default_user_agent = env_str(
    "PINK_TAX_OBF_USER_AGENT",
    "PinkTaxResearch/1.0 (academic; github.com/pink-tax-study)",
)
default_model_name = env_str("PINK_TAX_MODEL_NAME", "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")
default_model_threshold = env_float("PINK_TAX_MODEL_THRESHOLD", 0.60)

clean_max_abs_pink_tax = env_float("PINK_TAX_CLEAN_MAX_ABS_PINK_TAX", 400.0)
clean_min_match_quality = env_int("PINK_TAX_CLEAN_MIN_MATCH_QUALITY", 3)
clean_min_size_ratio = env_float("PINK_TAX_CLEAN_MIN_SIZE_RATIO", 0.25)
clean_max_size_ratio = env_float("PINK_TAX_CLEAN_MAX_SIZE_RATIO", 4.0)
clean_min_price_ratio = env_float("PINK_TAX_CLEAN_MIN_PRICE_RATIO", 0.1)
clean_max_price_ratio = env_float("PINK_TAX_CLEAN_MAX_PRICE_RATIO", 8.0)
clean_drop_from_column = env_str("PINK_TAX_CLEAN_DROP_FROM_COLUMN", "gender_model_name")

quality_min_city_pairs = env_int("PINK_TAX_QUALITY_MIN_CITY_PAIRS", 80)
quality_recommended_city_pairs = env_int("PINK_TAX_QUALITY_RECOMMENDED_CITY_PAIRS", 150)
quality_min_category_pairs = env_int("PINK_TAX_QUALITY_MIN_CATEGORY_PAIRS", 8)
quality_pink_tax_abs_threshold = env_float("PINK_TAX_QUALITY_PINK_TAX_ABS_THRESHOLD", 200.0)
quality_min_match_quality = env_int("PINK_TAX_QUALITY_MIN_MATCH_QUALITY", 3)

"""
Shared category GST map.
"""
default_gst_rates: dict[str, float] = {
    "Shampoo": 0.18,
    "Conditioner": 0.18,
    "Hair Oil": 0.18,
    "Body Wash": 0.18,
    "Bar Soap": 0.12,
    "Face Moisturizer": 0.18,
    "Facial Cleanser": 0.18,
    "Body Lotion": 0.18,
    "Hand Cream": 0.18,
    "Deodorant Spray": 0.18,
    "Deodorant Roll-On": 0.18,
    "Sunscreen": 0.18,
    "Toothpaste": 0.12,
    "Razor (3-blade starter kit)": 0.12,
    "Razor Cartridges": 0.12,
    "Hair Colour": 0.18,
    "Hair Gel / Serum": 0.18,
    "Hand Wash": 0.18,
    "Face Toner": 0.18,
}

"""
Backward-compatible constant aliases.
"""
default_data_date = default_date
default_obf_sleep_seconds = default_sleep_sec
default_obf_search_url = default_obf_search_url
default_obf_user_agent = default_user_agent
default_model_name = default_model_name
default_model_threshold = default_model_threshold
default_gst_rates = default_gst_rates
default_clean_max_abs_pink_tax = clean_max_abs_pink_tax
default_clean_min_match_quality = clean_min_match_quality
default_clean_min_size_ratio = clean_min_size_ratio
default_clean_max_size_ratio = clean_max_size_ratio
default_clean_min_price_ratio = clean_min_price_ratio
default_clean_max_price_ratio = clean_max_price_ratio
default_clean_drop_from_column = clean_drop_from_column
default_quality_min_city_pairs = quality_min_city_pairs
default_quality_recommended_city_pairs = quality_recommended_city_pairs
default_quality_min_category_pairs = quality_min_category_pairs
default_quality_pink_tax_abs_threshold = quality_pink_tax_abs_threshold
default_quality_min_match_quality = quality_min_match_quality

"""
Backward-compatible snake_case aliases.
"""
default_data_date = default_date
default_sleep_seconds = default_sleep_sec

def load_pipeline_definition(path: Path | None = None) -> dict:
    """
    Load the pipeline step definition JSON.
    """

    cfg_path = path or get_paths().pipeline_steps
    with cfg_path.open(encoding="utf-8") as f:
        return json.load(f)
