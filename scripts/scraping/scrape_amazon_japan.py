"""
Uses Amazon.co.jp search to find real product URLs automatically.
"""

from datetime import date
from pathlib import Path
from urllib.parse import quote
import csv
import time
import random
import re
import argparse
import logging
import sys
import importlib
from typing import Any

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import default_model_name, default_model_threshold, get_paths
from pink_tax.scraping_config import load_scraping_source_config
from pink_tax.scraping_utils.gender_labeler import ModelGenderLabeler

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
source_config = load_scraping_source_config(root, "amazon_japan")
output_path = source_config["output_path"]
found_urls_path = source_config["found_urls_path"]
city = source_config["city"]
currency = source_config["currency"]
retailer = source_config["retailer"]
today = str(date.today())
user_agents = source_config["user_agents"]
target_products = source_config["target_products"]
search_base_url = source_config["search_base_url"]
product_url_template = source_config["product_url_template"]
referer_url = source_config["referer_url"]
accept_language = source_config["accept_language"]
request_timeout_seconds = float(source_config.get("request_timeout_seconds", 15))

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

def search_amazon_jp(query: str, session: Any) -> str | None:
    """
    Search Amazon.co.jp and return the URL of the first result.
    """

    search_url = search_base_url.format(query=quote(query))
    headers = {
        "User-Agent":      random.choice(user_agents),
        "Accept-Language": accept_language,
        "Accept":          "text/html,application/xhtml+xml",
        "Referer":         referer_url,
    }

    try:
        resp = session.get(search_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            log.warning(f"  Search HTTP {resp.status_code} for: {query}")
            return None
        soup = beautiful_soup(resp.text, "html.parser")
        for el in soup.select("div[data-asin]"):
            asin = el.get("data-asin", "").strip()
            if len(asin) == 10:
                url = product_url_template.format(asin=asin)
                log.info(f"  Found ASIN {asin} → {url}")
                return url
        log.warning(f"  No ASIN found for: {query}")
        return None
    
    except request_exception as e:
        log.error(f"  Search failed: {e}")
        return None

def extract_price_jp(soup: Any) -> tuple[float | None, float | None, bool]:
    """
    Extract price from Amazon.co.jp page. JPY has no decimals: ¥1,298 = 1298.
    """

    price = None
    original_price = None
    on_promotion = False

    for selector, attrs in [
        ("span", {"class": "a-price-whole"}),
        ("span", {"id": "priceblock_ourprice"}),
        ("span", {"id": "priceblock_dealprice"}),
        ("span", {"class": "a-offscreen"}),
    ]:
        el = soup.find(selector, attrs)
        if el:
            raw = el.get_text(strip=True)
            raw = re.sub(r"[¥,\s円]", "", raw).split(".")[0].strip()
            try:
                price = float(raw)
                break
            except ValueError:
                continue

    strike = soup.find("span", {"class": "a-text-strike"})
    if strike:
        raw = re.sub(r"[¥,\s円]", "", strike.get_text(strip=True)).split(".")[0].strip()
        try:
            original_price = float(raw)
            on_promotion = True
        except ValueError:
            pass

    return price, original_price, on_promotion

def scrape_product(
    product: dict,
    session: Any,
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
        log.info(
            f"  [DRY] exp={row['expected_gender_label']:<7} pred={row['gender_label']:<7} "
            f"conf={row['model_gender_confidence']:.3f}  {product['product_name']}"
        )
        log.info(f"         search: {product.get('search_query', '')}")
        return row

    url = product.get("url")
    if not url:
        log.info(f"  Searching: {product['search_query']}")
        url = search_amazon_jp(product["search_query"], session)
        time.sleep(random.uniform(2.0, 4.0))

    if not url:
        row["scrape_status"] = "URL_NOT_FOUND"
        return row

    row["source_url"] = url

    try:
        headers = {
            "User-Agent":      random.choice(user_agents),
            "Accept-Language": accept_language,
            "Accept":          "text/html,application/xhtml+xml",
            "Referer":         referer_url,
        }
        resp = session.get(url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            row["scrape_status"] = f"HTTP_{resp.status_code}"
            return row

        soup = beautiful_soup(resp.text, "html.parser")
        price, orig, promo = extract_price_jp(soup)

        if price is None:
            row["scrape_status"] = "PRICE_NOT_FOUND"
            log.warning(f"  MISS: {product['product_name']}")
        else:
            row["price_local"]          = price
            row["original_price_local"] = orig
            row["on_promotion"]         = promo
            row["confidence"]           = "HIGH"
            log.info(
                f"  ✓ {row['gender_label']:<7} ({row['model_gender_confidence']:.3f}) "
                f"{product['product_name']:<50} ¥{price:.0f}"
            )

    except request_exception as e:
        log.error(f"  Request failed: {e}")
        row["scrape_status"] = "REQUEST_ERROR"

    return row

def main(
    dry_run: bool = False,
    model_name: str = default_model_name,
    model_threshold: float = default_model_threshold,
):
    if not dependencies_ok:
        print("ERROR: requests + beautifulsoup4 are required. Run: pip install requests beautifulsoup4")
        return

    paths.data_raw.mkdir(parents=True, exist_ok=True)
    session = requests_module.Session()
    labeler = ModelGenderLabeler(
        model_name=model_name,
        cache_path=paths.data_raw / "gender_model_cache.json",
        threshold=model_threshold,
    )
    results = []
    found_urls = []

    log.info(f"Amazon.co.jp scrape — {len(target_products)} products {'[DRY RUN]' if dry_run else ''}")

    for i, product in enumerate(target_products, 1):
        log.info(f"\n[{i:>2}/{len(target_products)}] {product['product_name']}")
        row = scrape_product(product, session, labeler, dry_run=dry_run)
        results.append(row)
        if row.get("source_url") and not dry_run:
            found_urls.append(f"{row['pair_id']}|{row['expected_gender_label']}|{row['source_url']}")
        if not dry_run:
            delay = random.uniform(5.0, 10.0)
            log.info(f"  Sleeping {delay:.1f}s...")
            time.sleep(delay)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    labeler.persist()

    if found_urls:
        with open(found_urls_path, "w") as f:
            f.write("pair_id | gender | url\n\n")
            f.write("\n".join(found_urls))
        log.info(f"\nURLs saved → {found_urls_path}")

    ok   = sum(1 for r in results if r["scrape_status"] == "OK")
    fail = len(results) - ok
    log.info(f"\nDone. OK={ok}  Failed={fail}  Output → {output_path}")

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
