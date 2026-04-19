# Pink Tax Comparison Repository

This repository builds a real-price dataset for pink tax analysis in Hyderabad and Tokyo.
The pipeline is organized into two technical phases:

- `build`: scrape product prices and assemble female/male same-brand observations
- `clean`: compute pink tax features, apply quality filters, and enrich ingredients

The project is designed for assignment delivery with reproducible scripts, raw and clean outputs, and documentation.

## 1) Repository Purpose

The main output is a cleaned paired-product dataset at:

- `data/clean/pink_tax_final_dataset_cleaned.csv`

Each row represents one female vs male matched product observation with derived metrics such as:

- female and male price per unit (`female_ppu_local`, `male_ppu_local`)
- pink tax percentage (`pink_tax_pct`)
- quality metadata (`match_quality`, `confidence`)

## 2) End-to-End Data Flow

1. Build/refresh seed catalog (`data/spec/pair_seed_catalog.csv`)
2. Reset previous generated outputs (`data/raw/*.csv`, URL caches, clean outputs)
3. Run scrapers per source
4. Merge source raw files into one paired observation spec (`data/raw/pair_observations.csv`)
5. Generate full labeled dataset with model + keyword gender diagnostics
6. Apply strict cleaning filters (dedupe, missing, numeric validity, outlier bounds)
7. Produce quality summary metrics
8. Enrich ingredient overlap via Open Beauty Facts

## 3) Directory Guide

### `config/`

- `pipeline_steps.json`: full pipeline (`all`)
- `pipeline_build_steps.json`: build-only steps
- `pipeline_clean_steps.json`: clean-only steps
- `scraping/*.json`: per-source scraping settings (URLs, delays, timeouts, cache paths)
- `cleaning/obf_fallback_ingredients.json`: curated fallback ingredient mappings

### `scripts/scraping/`

Source scrapers writing raw outputs into `data/raw/`:

- `scrape_amazon_india.py` -> `amazon_in_raw.csv`
- `scrape_bigbasket.py` -> `bigbasket_raw.csv` and `blinkit_raw.csv`
- `scrape_flipkart.py` -> `flipkart_raw.csv`
- `scrape_amazon_japan.py` -> `amazon_jp_raw.csv`
- `scrape_rakuten_japan.py` -> `rakuten_jp_raw.csv` (optional step)
- `scrape_matsumoto_kiyoshi.py` -> `matsumoto_raw.csv` (optional step)

Raw rows include `scrape_status` such as `OK`, `URL_NOT_FOUND`, `PRICE_NOT_FOUND`, `BLOCKED`, `REQUEST_ERROR`.

### `scripts/cleaning/`

- `expand_pair_seed_catalog.py`: expands seed pairs for scraping coverage
- `build_pair_observations.py`: merges raw scraper outputs into paired observations
- `generate_pairs_dataset.py`: computes features and runs hybrid gender labeling
- `clean_pairs_dataset.py`: applies strict cleaning filters and trims analysis columns
- `build_quality_queue.py`: outputs quality summary metrics CSV
- `enrich_openbeautyfacts.py`: adds ingredient overlap metrics using API + fallback table

### `scripts/pipeline/`

- `run_pipeline.py`: sequential step runner
- `reset_outputs.py`: wipes generated outputs before fresh build
- `run_all_after_scraper_dryrun.py`: optional gate that runs dry-runs first

### `src/pink_tax/`

Shared internal library:

- `config.py`: global paths + environment variable defaults
- `scraping_config.py`: scraping JSON loader helpers
- `utils.py`: shared utilities (parsing, backups, selection helpers)
- `scraping_utils/`: normalization, labeling, matching helpers

### `data/`

- `data/spec/`: seed catalog and specification inputs
- `data/raw/`: per-source raw scraper output + merged paired observations
- `data/clean/`: cleaned final dataset + quality summary
- `data/raw/_backups` and `data/clean/_backups`: automatic backups on overwrite

## 4) Run Instructions

### Prerequisites

```bash
python -m pip install -r requirements.txt
```

If you use browser-mode scraping, ensure browser automation dependencies are installed in your active interpreter.

### Default full run (recommended)

```bash
python run_all.py
```

This runs mode `all` and skips optional steps by default.

### Include optional scrapers (Rakuten + Matsumoto)

```bash
python run_all.py --include-optional
```

### Build only

```bash
python run_all.py --mode build
```

### Clean only

```bash
python run_all.py --mode clean
```

### Single step

```bash
python run_all.py --mode all --step build_observations
```

## 5) Optional and Source Reliability

- `scrape_rakuten_jp` and `scrape_matsumoto` are marked optional in pipeline configs.
- This keeps default runs stable when those sources are slow, blocked, or low-coverage.
- You can still run them manually and then continue from `build_observations`.

## 6) What Cleaning Actually Does

### `build_pair_observations.py`

- Groups by `(pair_code, city, brand, category, retailer, date)`
- Keeps only complete female+male sides
- Requires positive prices and positive sizes on both sides
- Scores multiple side candidates by:
  - price presence
  - confidence level
  - match quality

### `generate_pairs_dataset.py`

- Computes:
  - `female_ppu_local`
  - `male_ppu_local`
  - `pink_tax_pct`
- Runs hybrid gender labeling:
  - keyword normalization layer
  - model classifier (`MoritzLaurer/mDeBERTa-v3-base-mnli-xnli`)
- Stores review flags and label provenance

### `clean_pairs_dataset.py`

- Deduplicates by strict composite key
- Removes rows with missing required fields
- Removes rows with invalid/nonpositive numeric values
- Applies threshold filters:
  - min match quality
  - max absolute pink tax
  - size ratio bounds
  - price ratio bounds
- Trims columns from configurable start column (`gender_model_name` by default)

### `build_quality_queue.py`

- Generates summary metrics and issue counts (no row rewrite)
- Checks consistency and risk signals:
  - city/currency consistency
  - retailer-city validity
  - date validity
  - duplicate identity rows
  - suspicious price/size ratios
  - review flags

### `enrich_openbeautyfacts.py`

- Adds ingredient overlap metrics from Open Beauty Facts
- Uses API cache at `data/raw/obf_cache.json`
- Falls back to curated ingredient mappings when API match is weak

## 7) Output Files and Meaning

### Core generated outputs

- `data/raw/amazon_in_raw.csv`, `amazon_jp_raw.csv`, `flipkart_raw.csv`, `bigbasket_raw.csv`, `blinkit_raw.csv`, `rakuten_jp_raw.csv`, `matsumoto_raw.csv`
- `data/raw/pair_observations.csv`: merged paired observations used as dataset source
- `data/clean/pink_tax_final_dataset_cleaned.csv`: final cleaned analysis dataset
- `data/clean/pink_tax_quality_review_summary.csv`: quality summary metrics

### Important note

- Raw files can contain non-OK rows (`URL_NOT_FOUND`, `PRICE_NOT_FOUND`, etc.).
- Final cleaned dataset excludes invalid/unpaired rows through the pairing + cleaning pipeline.

## 8) Environment and Configuration

Global settings are loaded from `.env` via `src/pink_tax/config.py`.

Common keys:

- `PINK_TAX_MODEL_NAME`
- `PINK_TAX_MODEL_THRESHOLD`
- `PINK_TAX_CLEAN_MAX_ABS_PINK_TAX`
- `PINK_TAX_CLEAN_MIN_MATCH_QUALITY`
- `PINK_TAX_QUALITY_MIN_CITY_PAIRS`
- `PINK_TAX_QUALITY_PINK_TAX_ABS_THRESHOLD`

Per-source scraper config override:

- `PINK_TAX_SCRAPER_CONFIG_<SOURCE>` where `<SOURCE>` is upper-case source key, for example `RAKUTEN_JAPAN`.

## 9) Assignment Artifacts in Repo

- Final cleaned dataset CSV
- Raw per-source CSVs
- Quality summary CSV
- Codebook PDF (`Codebook.pdf`)
- Additional writing/docs:
  - `ASSIGNMENT1_DATA_QUALITY.md`
  - `REPOSITORY_DIAGRAM.md`

## 10) Troubleshooting

- Blocked scraper: run source manually in browser mode with `--headful`, then `--resume`.
- Empty or weak source output: keep source optional and proceed with stable sources.
- Fresh run from zero outputs:
  - use `python run_all.py` (includes reset step in build/all modes).

## 11) Canonical Script Locations

The canonical pipeline scripts are under `scripts/`.
If duplicate script copies exist at repository root, pipeline execution still uses `scripts/...` paths from pipeline JSON configs.

## 12) EDA + Website

This repo includes a full exploratory analysis pipeline and a deployable frontend site.

### EDA pipeline

- Existing artifact: `frontend/public/data/eda_summary.json` (filter facets + headline metrics consumed by the React app).
- Statistical aggregations cited in the paper now live alongside the regression output in `data/analysis/regression_summary.json` and are produced by `scripts/analysis/run_regression.py` (see next subsection). The Statistics tab of the React app loads from that JSON.

### Regression confirmation (OLS)

- Script: `scripts/analysis/run_regression.py`
- Input: `data/clean/pink_tax_final_dataset_cleaned.csv`
- Output: `data/analysis/regression_summary.json` (also copy to `frontend/public/data/` and `website/data/` for static sites)

Run:

```bash
python scripts/analysis/run_regression.py
cp data/analysis/regression_summary.json frontend/public/data/regression_summary.json
cp data/analysis/regression_summary.json website/data/regression_summary.json
```

The script runs hypothesis tests (paired and city comparisons), several OLS specifications on `pink_tax_pct` (including price level, match quality, size ratio, and ingredient controls on the OBF subset), a logit for P(pink tax > 0), and writes narrative conclusions into the JSON. Use the **Statistics & conclusions** tab in the React app to read them, or open `data/analysis/regression_summary.json`.

The same script also emits the descriptive aggregations cited in the paper (`Gendered at a Price`, Section 5 and Table 5):

- `descriptive.distribution_overall` — mean, median, SD, IQR, P10/P90, min/max
- `descriptive.direction_overall` — share of pairs with women paying more / men paying more / parity
- `descriptive.by_city` — per-city distribution, one-sample t-test vs 0 with 95% CI, and direction shares (Section 5.2)
- `descriptive.category_table` — Table 3: n / mean / SD / t-stat / p for every category
- `descriptive.city_category_diff` — Tokyo − Hyderabad mean per category (Figure 3)
- `descriptive.retailer_summary` — Section 5.5 retailer means, medians, P90s, direction shares
- `descriptive.brand_summary` — Section 5.4 brand-level means (n ≥ 5)
- `descriptive.ingredient_overlap_buckets` — Table 4 (the headline finding)
- `descriptive.size_ratio_breakdown` — Section 4.3 pack-size matching summary
- `descriptive.cleaning_funnel` — Table 2

Regression `models.m4_paper_city_category_retailer_overlap_hc3` corresponds exactly to **M4 in Table 5** of the paper (M3 + continuous ingredient overlap on n=273 with HC3 robust SEs).

### EDA notebook

- Notebook: `notebooks/exploratory_data_analysis.ipynb`
- Purpose:
  - reproducible exploratory workflow
  - data loading and sample checks
  - descriptive statistics table
  - core EDA figures with short interpretation and limitations

Open/run from repository root:

```bash
jupyter notebook notebooks/exploratory_data_analysis.ipynb
```

or in VS Code:

```bash
code notebooks/exploratory_data_analysis.ipynb
```

### Frontend website

- React app folder: `frontend/`
- React data feed: `frontend/public/data/eda_summary.json`

Run React frontend locally:

```bash
cd frontend
npm install
npm run dev -- --host
```

Then open:

- `http://localhost:5173`

Optional static fallback:

- `website/` (plain HTML version)

Run static fallback locally:

```bash
python -m http.server 8080 --directory website
```

Then open:

- `http://localhost:8080`
