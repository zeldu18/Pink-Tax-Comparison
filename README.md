# Pink Tax Dataset Pipeline

Minimal repository focused only on:
- scraping product observations
- classification-based gender labeling
- dataset creation
- cleaning and quality summary generation

## Quick Start

```bash
python -m pip install -r requirements.txt
python run_all.py --mode all
```

This full run sequence does:
1. scrape Amazon India
2. scrape BigBasket
3. scrape Amazon Japan
4. scrape Matsumoto Kiyoshi
5. merge scraper outputs into `data/raw/pair_observations.csv`
6. generate labeled pairs dataset
7. finalize cleaning (dedupe/missing/outlier filters)
8. build quality summary `data/clean/pink_tax_quality_review_summary.csv`
9. enrich ingredients using Open Beauty Facts

## Stage Split

Build-only stage:
```bash
python run_all.py --mode build
```

Clean-only stage:
```bash
python run_all.py --mode clean
```

Run one step:
```bash
python run_all.py --mode clean --step clean_finalize
```

## Config Layout
```text
.
├── run_all.py
├── config/
│   ├── pipeline_steps.json
│   ├── pipeline_build_steps.json
│   ├── pipeline_clean_steps.json
│   ├── scraping/
│   │   ├── amazon_in.json
│   │   ├── amazon_japan.json
│   │   ├── bigbasket.json
│   │   └── matsumoto_kiyoshi.json
│   └── cleaning/
│       └── obf_fallback_ingredients.json
├── scripts/
│   ├── scraping/
│   │   ├── scrape_amazon_india.py
│   │   ├── scrape_bigbasket.py
│   │   ├── scrape_amazon_japan.py
│   │   └── scrape_matsumoto_kiyoshi.py
│   ├── cleaning/
│   │   ├── build_pair_observations.py
│   │   ├── generate_pairs_dataset.py
│   │   ├── clean_pairs_dataset.py
│   │   ├── build_quality_queue.py
│   │   └── enrich_openbeautyfacts.py
│   └── pipeline/
│       └── run_pipeline.py
├── src/pink_tax/
│   ├── config.py
│   ├── scraping_config.py
│   ├── utils.py
│   └── scraping_utils/
└── data/
    ├── raw/
    └── clean/
```
