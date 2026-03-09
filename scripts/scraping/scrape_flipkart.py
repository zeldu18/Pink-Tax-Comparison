"""
Flipkart scraper with OBF-seeded target expansion.
"""

from __future__ import annotations
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus, urljoin
import argparse
import csv
import importlib
import logging
import random
import re
import sys
import time
from typing import Any

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

requests_module: Any = None
beautiful_soup: Any = None
request_exception: type[Exception] = Exception

try:
    requests_module = importlib.import_module("requests")
    beautiful_soup = importlib.import_module("bs4").BeautifulSoup
    request_exception = requests_module.RequestException
    dependencies_ok = True
except ImportError:
    dependencies_ok = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)
paths = get_paths(root)

source_config = load_scraping_source_config(root, "flipkart")
output_path = source_config["output_path"]
found_urls_path = source_config["found_urls_path"]
city = source_config["city"]
currency = source_config["currency"]
retailer = source_config["retailer"]
today = str(date.today())
user_agents = source_config["user_agents"]
target_products = source_config["target_products"]
search_base_url = source_config["search_base_url"]
referer_url = source_config["referer_url"]
accept_language = source_config["accept_language"]
request_timeout_seconds = float(source_config.get("request_timeout_seconds", 15))
auto_seed_from_obf = bool(source_config.get("auto_seed_from_obf", True))
auto_seed_max_pairs = int(source_config.get("auto_seed_max_pairs", 180))
auto_seed_locale = str(source_config.get("auto_seed_locale", "in"))
search_pause_min_seconds = float(source_config.get("search_pause_min_seconds", 0.8))
search_pause_max_seconds = float(source_config.get("search_pause_max_seconds", 1.8))
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

def search_flipkart(query: str, session: Any) -> str | None:
    """
    Search Flipkart and return first product URL.
    """

    search_url = search_base_url.format(query=quote_plus(query))
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": accept_language,
        "Accept": "text/html,application/xhtml+xml",
        "Referer": referer_url,
    }

    try:
        resp = session.get(search_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            log.warning(f"Search HTTP {resp.status_code} for: {query}")
            return None

        soup = beautiful_soup(resp.text, "html.parser")
        for anchor in soup.select("a[href]"):
            href = str(anchor.get("href") or "").strip()
            if "/p/" in href:
                return urljoin("https://www.flipkart.com", href)
    except request_exception as exc:
        log.error(f"Search request failed: {exc}")

    return None

def extract_price_flipkart(soup: Any) -> tuple[float | None, float | None, bool]:
    """
    Extract current and original prices from Flipkart product page.
    """

    text = soup.get_text(" ", strip=True)
    amounts = re.findall(r"₹\s*([0-9][0-9,]*)", text)
    values: list[float] = []
    for raw in amounts:
        try:
            values.append(float(raw.replace(",", "")))
        except ValueError:
            continue

    if not values:
        return None, None, False

    price = values[0]
    original_price = None
    on_promo = False
    if len(values) > 1 and values[1] > price:
        original_price = values[1]
        on_promo = True
    return price, original_price, on_promo

def scrape_product(
    product: dict,
    session: Any,
    labeler: ModelGenderLabeler,
    dry_run: bool = False,
) -> dict:
    """
    Scrape one product observation row.
    """

    gender_meta = labeler.classify(
        product_name=product["product_name"],
        expected_label=product.get("gender_label", ""),
    )
    row = {
        "pair_id": product["pair_id"],
        "city": city,
        "brand": product["brand"],
        "category": product["category"],
        "expected_gender_label": gender_meta["expected_gender_label"],
        "gender_label": gender_meta["gender_label"],
        "gender_label_source": gender_meta["gender_label_source"],
        "model_gender_label": gender_meta["model_gender_label"],
        "model_gender_confidence": gender_meta["model_gender_confidence"],
        "keyword_gender_label": gender_meta["keyword_gender_label"],
        "keyword_evidence": gender_meta["keyword_evidence"],
        "gender_needs_review": gender_meta["gender_needs_review"],
        "gender_model_name": gender_meta["gender_model_name"],
        "gender_model_threshold": gender_meta["gender_model_threshold"],
        "product_name": product["product_name"],
        "size_ml_or_g": product["size_ml_or_g"],
        "price_local": None,
        "currency": currency,
        "original_price_local": None,
        "on_promotion": False,
        "retailer": retailer,
        "match_quality": product["match_quality"],
        "confidence": "LOW",
        "date_scraped": today,
        "source_url": str(product.get("url") or "").strip(),
        "scrape_status": "OK",
        "ingredients": product.get("ingredients", ""),
    }

    if dry_run:
        row["price_local"] = "DRY_RUN"
        row["scrape_status"] = "DRY_RUN"
        log.info(
            f"[DRY RUN] exp={row['expected_gender_label']:<7} pred={row['gender_label']:<7} "
            f"conf={row['model_gender_confidence']:.3f}  {product['product_name']}"
        )
        log.info(f"search: {product.get('search_query', '')}")
        return row

    url = str(product.get("url") or "").strip()
    if not url:
        query = str(product.get("search_query") or product["product_name"]).strip()
        log.info(f"Searching: {query}")
        url = search_flipkart(query, session) or ""
        time.sleep(random.uniform(search_pause_min_seconds, search_pause_max_seconds))

    if not url:
        row["scrape_status"] = "URL_NOT_FOUND"
        return row

    row["source_url"] = url

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": accept_language,
        "Accept": "text/html,application/xhtml+xml",
        "Referer": referer_url,
    }
    try:
        resp = session.get(url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            row["scrape_status"] = f"HTTP_{resp.status_code}"
            return row

        soup = beautiful_soup(resp.text, "html.parser")
        price, original_price, on_promotion = extract_price_flipkart(soup)
        if price is None:
            row["scrape_status"] = "PRICE_NOT_FOUND"
            return row

        row["price_local"] = price
        row["original_price_local"] = original_price
        row["on_promotion"] = on_promotion
        row["confidence"] = "HIGH"
        log.info(
            f"✓ {row['gender_label']:<7} ({row['model_gender_confidence']:.3f}) "
            f"{product['product_name']:<50} ₹{price}"
        )
    except request_exception as exc:
        row["scrape_status"] = "REQUEST_ERROR"
        log.error(f"Request failed: {exc}")

    return row

def main(
    dry_run: bool = False,
    model_name: str = default_model_name,
    model_threshold: float = default_model_threshold,
) -> None:
    """
    CLI entrypoint.
    """

    if not dependencies_ok:
        print("ERROR: requests + beautifulsoup4 are required. Run: pip install requests beautifulsoup4")
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
    session = requests_module.Session()
    labeler = ModelGenderLabeler(
        model_name=model_name,
        cache_path=paths.data_raw / "gender_model_cache.json",
        threshold=model_threshold,
    )

    results: list[dict] = []
    found_urls: list[str] = []
    log.info(f"Flipkart scrape — {len(seed_targets)} products {'[DRY RUN]' if dry_run else ''}")

    for idx, product in enumerate(seed_targets, start=1):
        log.info(f"\n[{idx:>3}/{len(seed_targets)}] {product['product_name']}")
        row = scrape_product(product=product, session=session, labeler=labeler, dry_run=dry_run)
        results.append(row)
        if row.get("source_url") and not dry_run:
            found_urls.append(f"{row['pair_id']}|{row['expected_gender_label']}|{row['source_url']}")

        if not dry_run:
            delay = random.uniform(step_delay_min_seconds, step_delay_max_seconds)
            time.sleep(delay)

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    labeler.persist()

    if found_urls:
        with open(found_urls_path, "w", encoding="utf-8") as handle:
            handle.write("pair_id|gender|url\n")
            handle.write("\n".join(found_urls))
        log.info(f"URLs saved → {found_urls_path}")

    ok_rows = sum(1 for row in results if row["scrape_status"] == "OK")
    fail_rows = len(results) - ok_rows
    log.info(f"Done. OK={ok_rows}  Failed={fail_rows}  Output → {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-name", default=default_model_name)
    parser.add_argument("--model-threshold", default=default_model_threshold, type=float)
    cli_args = parser.parse_args()
    main(
        dry_run=cli_args.dry_run,
        model_name=cli_args.model_name,
        model_threshold=cli_args.model_threshold,
    )