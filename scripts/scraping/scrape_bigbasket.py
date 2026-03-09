"""
BigBasket is a JavaScript-rendered site. 
Selenium (a real Chrome browser) is used to wait for the price element.
"""

import csv
import time
import random
import argparse
import logging
import sys
import importlib
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

webdriver: Any = None
options_class: Any = None
by_class: Any = None
no_such_element_exception: type[Exception] = Exception

try:
    webdriver = importlib.import_module("selenium.webdriver")
    options_class = importlib.import_module("selenium.webdriver.chrome.options").Options
    by_class = importlib.import_module("selenium.webdriver.common.by").By
    no_such_element_exception = importlib.import_module(
        "selenium.common.exceptions"
    ).NoSuchElementException
    selenium_ok = True
except ImportError:
    selenium_ok = False

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import default_model_name, default_model_threshold, get_paths
from pink_tax.scraping_config import load_scraping_source_config
from pink_tax.scraping_utils.gender_labeler import ModelGenderLabeler
from pink_tax.scraping_utils.obf_seed_loader import (
    build_targets_from_obf_cache,
    merge_target_products,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)
paths = get_paths(root)

source_config = load_scraping_source_config(root, "bigbasket")
output_path = source_config["output_path"]
found_urls_path = source_config.get("found_urls_path", "")
city = source_config["city"]
currency = source_config["currency"]
retailer = source_config["retailer"]
today = str(date.today())
target_products = source_config["target_products"]
search_base_url = str(source_config.get("search_base_url", "")).strip()
auto_seed_from_obf = bool(source_config.get("auto_seed_from_obf", True))
auto_seed_max_pairs = int(source_config.get("auto_seed_max_pairs", 120))
auto_seed_locale = str(source_config.get("auto_seed_locale", "in"))
search_pause_min_seconds = float(source_config.get("search_pause_min_seconds", 1.5))
search_pause_max_seconds = float(source_config.get("search_pause_max_seconds", 3.0))
step_delay_min_seconds = float(source_config.get("step_delay_min_seconds", 1.5))
step_delay_max_seconds = float(source_config.get("step_delay_max_seconds", 3.5))

fieldnames = [
    "pair_id", "city", "brand", "category",
    "expected_gender_label", "gender_label", "gender_label_source",
    "model_gender_label", "model_gender_confidence",
    "keyword_gender_label", "keyword_evidence", "gender_needs_review",
    "gender_model_name", "gender_model_threshold",
    "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status", "ingredients",
]

def build_driver() -> Any:
    """
    Build a headless Chrome driver.
    """

    opts = options_class()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    try:
        chrome_manager = importlib.import_module("webdriver_manager.chrome").ChromeDriverManager
        service_class = importlib.import_module("selenium.webdriver.chrome.service").Service
        return webdriver.Chrome(
            service=service_class(chrome_manager().install()),
            options=opts
        )
    except ImportError:
        return webdriver.Chrome(options=opts)

def extract_price_bigbasket(driver: Any) -> tuple[float | None, float | None, bool]:
    """
    Extract price from BigBasket product page.
    """

    price = None
    original_price = None
    on_promotion = False

    try:
        price_selectors = [
            "span.discnt-price",
            "span.selling-price",
            "[qa='discounted-price']",
            "[qa='selling-price']",
        ]

        for selector in price_selectors:
            try:
                el = driver.find_element(by_class.CSS_SELECTOR, selector)
                raw = el.text.strip().replace("₹", "").replace(",", "").strip()
                price = float(raw)
                break
            except (no_such_element_exception, ValueError):
                continue

        try:
            mrp_el = driver.find_element(
                by_class.CSS_SELECTOR,
                "span.mrp-price, span.discnt-price-w-o",
            )
            raw = mrp_el.text.strip().replace("₹", "").replace(",", "").strip()
            original_price = float(raw)
            if original_price and price and original_price > price:
                on_promotion = True
        except (no_such_element_exception, ValueError):
            pass

    except Exception as e:
        log.warning(f"Price extraction error: {e}")

    return price, original_price, on_promotion

def search_bigbasket_url(driver: Any, query: str) -> str | None:
    """
    Search BigBasket and return first product URL.
    """

    if not search_base_url:
        return None

    search_url = search_base_url.format(query=quote_plus(str(query or "").strip()))
    try:
        driver.get(search_url)
        time.sleep(random.uniform(search_pause_min_seconds, search_pause_max_seconds))
        links = driver.find_elements(by_class.CSS_SELECTOR, "a[href*='/pd/']")
        for link in links:
            href = str(link.get_attribute("href") or "").strip()
            if "/pd/" in href:
                return href.split("?")[0]
    except Exception as exc:
        log.warning(f"BigBasket search failed for query '{query}': {exc}")

    return None

def scrape_product(
    product: dict,
    driver,
    labeler: ModelGenderLabeler,
    dry_run: bool = False,
) -> dict:
    
    gender_meta = labeler.classify(
        product_name=product["product_name"],
        expected_label=product.get("gender_label", ""),
    )

    row = {
        "pair_id":              product["pair_id"],
        "city":                 city,
        "brand":                product["brand"],
        "category":             product["category"],
        "expected_gender_label": gender_meta["expected_gender_label"],
        "gender_label":         gender_meta["gender_label"],
        "gender_label_source":  gender_meta["gender_label_source"],
        "model_gender_label":   gender_meta["model_gender_label"],
        "model_gender_confidence": gender_meta["model_gender_confidence"],
        "keyword_gender_label": gender_meta["keyword_gender_label"],
        "keyword_evidence":     gender_meta["keyword_evidence"],
        "gender_needs_review":  gender_meta["gender_needs_review"],
        "gender_model_name":    gender_meta["gender_model_name"],
        "gender_model_threshold": gender_meta["gender_model_threshold"],
        "product_name":         product["product_name"],
        "size_ml_or_g":         product["size_ml_or_g"],
        "price_local":          None,
        "currency":             currency,
        "original_price_local": None,
        "on_promotion":         False,
        "retailer":             retailer,
        "match_quality":        product["match_quality"],
        "confidence":           "LOW",
        "date_scraped":         today,
        "source_url":           product.get("url") or "",
        "scrape_status":        "OK",
        "ingredients":          product.get("ingredients", ""),
    }

    if dry_run:
        row["price_local"]   = "DRY_RUN"
        row["scrape_status"] = "DRY_RUN"
        log.info(f"search: {product.get('search_query', product['product_name'])}")
        log.info(
            f"[DRY RUN] exp={row['expected_gender_label']:<7} pred={row['gender_label']:<7} "
            f"conf={row['model_gender_confidence']:.3f}  {product['product_name']}"
        )
        return row

    url = str(product.get("url") or "").strip()
    if not url:
        search_query = str(product.get("search_query") or product["product_name"]).strip()
        log.info(f"Searching BigBasket: {search_query}")
        url = search_bigbasket_url(driver, search_query) or ""

    if not url:
        row["scrape_status"] = "URL_NOT_FOUND"
        log.warning(f"URL not found: {product['product_name']}")
        return row

    row["source_url"] = url

    try:
        driver.get(url)
        time.sleep(random.uniform(2.5, 5.0))

        price, orig, promo = extract_price_bigbasket(driver)

        if price is None:
            log.warning(f"Price not found: {product['product_name']}")
            row["scrape_status"] = "PRICE_NOT_FOUND"
        else:
            row["price_local"]          = price
            row["original_price_local"] = orig
            row["on_promotion"]         = promo
            row["confidence"]           = "HIGH"
            log.info(
                f"✓ {row['gender_label']:<7} ({row['model_gender_confidence']:.3f}) "
                f"{product['product_name']:<45} ₹{price}"
            )

    except Exception as e:
        log.error(f"Error scraping {url}: {e}")
        row["scrape_status"] = "ERROR"

    return row

def main(
    dry_run: bool = False,
    model_name: str = default_model_name,
    model_threshold: float = default_model_threshold,
):
    if not dry_run and not selenium_ok:
        print("ERROR: selenium is not installed. Run: pip install selenium")
        return

    seed_targets = list(target_products)
    if auto_seed_from_obf:
        obf_targets = build_targets_from_obf_cache(
            obf_cache_path=paths.obf_cache,
            city=city,
            locale=auto_seed_locale,
            max_pairs=auto_seed_max_pairs,
            min_match_quality=3,
        )
        seed_targets = merge_target_products(seed_targets, obf_targets)
        log.info(
            f"OBF seed merge enabled: base={len(target_products)} merged={len(seed_targets)} "
            f"(added={len(seed_targets) - len(target_products)})"
        )

    paths.data_raw.mkdir(parents=True, exist_ok=True)
    labeler = ModelGenderLabeler(
        model_name=model_name,
        cache_path=paths.data_raw / "gender_model_cache.json",
        threshold=model_threshold,
    )
    driver = None

    try:
        if not dry_run:
            log.info("Starting Chrome headless browser...")
            driver = build_driver()

        results = []
        log.info(f"BigBasket scrape: {len(seed_targets)} products "
                 f"{'[DRY RUN]' if dry_run else ''}")
        found_urls: list[str] = []

        for product in seed_targets:
            row = scrape_product(product, driver, labeler, dry_run=dry_run)
            results.append(row)
            if row.get("source_url") and row["source_url"] != "" and not dry_run:
                found_urls.append(
                    f"{row['pair_id']}|{row['expected_gender_label']}|{row['source_url']}"
                )

            if not dry_run:
                time.sleep(random.uniform(step_delay_min_seconds, step_delay_max_seconds))

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        labeler.persist()

        if found_urls_path and found_urls:
            with open(found_urls_path, "w", encoding="utf-8") as handle:
                handle.write("pair_id|gender|url\n")
                handle.write("\n".join(found_urls))
            log.info(f"Found URLs saved → {found_urls_path}")

        ok   = sum(1 for r in results if r["scrape_status"] == "OK")
        fail = len(results) - ok
        log.info(f"\nDone. OK={ok}  Failed={fail}  Written → {output_path}")

    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-name", default=default_model_name)
    parser.add_argument("--model-threshold", default=default_model_threshold, type=float)
    args = parser.parse_args()
    main(
        dry_run=args.dry_run,
        model_name=args.model_name,
        model_threshold=args.model_threshold,
    )
