"""
Matsumoto Kiyoshi is Japan's largest pharmacy chain. 
"""

from datetime import date
from pathlib import Path
import csv
import time
import random
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
request_timeout: type[Exception] = Exception
request_connection_error: type[Exception] = Exception

try:
    requests_module = importlib.import_module("requests")
    beautiful_soup = importlib.import_module("bs4").BeautifulSoup
    request_exception = requests_module.RequestException
    request_timeout = requests_module.Timeout
    request_connection_error = requests_module.ConnectionError
    dependencies_ok = True
except ImportError:
    dependencies_ok = False

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)
paths = get_paths(root)

source_config = load_scraping_source_config(root, "matsumoto_kiyoshi")
output_path = source_config["output_path"]
city = source_config["city"]
currency = source_config["currency"]
retailer = source_config["retailer"]
today = str(date.today())
default_timeout_seconds = source_config["default_timeout_seconds"]
default_max_retries = source_config["default_max_retries"]
default_retry_backoff_seconds = source_config["default_retry_backoff_seconds"]
default_retry_jitter_seconds = source_config["default_retry_jitter_seconds"]
default_abort_after_consecutive_request_errors = int(
    source_config.get("abort_after_consecutive_request_errors", 3)
)
default_reuse_existing_on_failure = bool(source_config.get("reuse_existing_on_failure", True))
preflight_url = source_config.get("preflight_url", "")
preflight_timeout_seconds = float(source_config.get("preflight_timeout_seconds", 6.0))
user_agents = source_config["user_agents"]
target_products = source_config["target_products"]
accept_language = source_config.get("accept_language", "ja-JP,ja;q=0.9")

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

def extract_price_matsumoto(soup: Any) -> tuple[float | None, float | None, bool]:
    """
    Extract price from Matsumoto Kiyoshi product page.
    """

    price = None
    original_price = None
    on_promotion = False

    selectors = [
        ("span", {"class": "price"}),
        ("p",    {"class": "price"}),
        ("span", {"itemprop": "price"}),
        ("div",  {"class": "price-box"}),
    ]

    for tag, attrs in selectors:
        el = soup.find(tag, attrs)
        if el:
            raw = el.get_text(strip=True)
            raw = raw.replace("¥", "").replace(",", "").replace("円", "").replace("税込", "").strip()
            import re
            m = re.search(r"(\d+)", raw)
            if m:
                try:
                    price = float(m.group(1))
                    break
                except ValueError:
                    continue

    original_el = soup.find("span", {"class": "original-price"}) or \
                  soup.find("del")
    if original_el and price:
        raw = original_el.get_text(strip=True).replace("¥", "").replace(",", "").strip()
        try:
            original_price = float(raw)
            if original_price > price:
                on_promotion = True
        except ValueError:
            pass

    return price, original_price, on_promotion

def fetch_with_retries(
    session: Any,
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_jitter_seconds: float,
) -> Any | None:
    """
    Fetch URL with retry/backoff for timeout and transient HTTP failures.
    """

    transient_codes = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout_seconds)
            if resp.status_code in transient_codes and attempt < max_retries:
                sleep_s = retry_backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, retry_jitter_seconds)
                log.warning(
                    f"Transient HTTP {resp.status_code} for {url} "
                    f"(attempt {attempt}/{max_retries}), retrying in {sleep_s:.1f}s"
                )
                time.sleep(sleep_s)
                continue
            return resp
        
        except request_timeout as e:
            if attempt >= max_retries:
                log.error(f"Request timeout after {max_retries} attempts: {e}")
                return None
            sleep_s = retry_backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, retry_jitter_seconds)
            log.warning(
                f"Request timeout (attempt {attempt}/{max_retries}) for {url}, retrying in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)

        except request_connection_error as e:
            if attempt >= max_retries:
                log.error(f"Connection error after {max_retries} attempts: {e}")
                return None
            sleep_s = retry_backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, retry_jitter_seconds)
            log.warning(
                f"Connection error (attempt {attempt}/{max_retries}) for {url}, retrying in {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)

        except request_exception as e:
            log.error(f"Request failed (non-retryable): {e}")
            return None
        
    return None

def scrape_product(
    product: dict,
    session: Any,
    labeler: ModelGenderLabeler,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_jitter_seconds: float,
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
        "source_url":           product["url"],
        "scrape_status":        "OK",
        "ingredients":          product.get("ingredients", ""),
    }

    if dry_run:
        row["price_local"]   = "DRY_RUN"
        row["scrape_status"] = "DRY_RUN"
        log.info(
            f"[DRY RUN] exp={row['expected_gender_label']:<7} pred={row['gender_label']:<7} "
            f"conf={row['model_gender_confidence']:.3f}  {product['product_name']}"
        )
        return row

    try:
        headers = {
            "User-Agent":      random.choice(user_agents),
            "Accept-Language": accept_language,
            "Accept":          "text/html,application/xhtml+xml",
        }
        resp = fetch_with_retries(
            session=session,
            url=product["url"],
            headers=headers,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
        )
        if resp is None:
            row["scrape_status"] = "REQUEST_ERROR"
            return row

        if resp.status_code != 200:
            row["scrape_status"] = f"HTTP_{resp.status_code}"
            return row

        soup = beautiful_soup(resp.text, "html.parser")
        price, orig, promo = extract_price_matsumoto(soup)

        if price is None:
            row["scrape_status"] = "PRICE_NOT_FOUND"
            log.warning(f"Price not found: {product['product_name']}")
        else:
            row["price_local"]          = price
            row["original_price_local"] = orig
            row["on_promotion"]         = promo
            row["confidence"]           = "HIGH"
            log.info(
                f"✓ {row['gender_label']:<7} ({row['model_gender_confidence']:.3f}) "
                f"{product['product_name']:<45} ¥{price:.0f}"
            )

    except request_exception as e:
        log.error(f"Request failed: {e}")
        row["scrape_status"] = "REQUEST_ERROR"

    return row


def load_existing_rows(path: str) -> list[dict[str, Any]]:
    """
    Load existing output rows if file exists.
    """

    output_file = Path(path)
    if not output_file.exists():
        return []

    with output_file.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))

def main(
    dry_run: bool = False,
    model_name: str = default_model_name,
    model_threshold: float = default_model_threshold,
    timeout_seconds: float = default_timeout_seconds,
    max_retries: int = default_max_retries,
    retry_backoff_seconds: float = default_retry_backoff_seconds,
    retry_jitter_seconds: float = default_retry_jitter_seconds,
    abort_after_consecutive_request_errors: int = default_abort_after_consecutive_request_errors,
    reuse_existing_on_failure: bool = default_reuse_existing_on_failure,
):
    if not dependencies_ok:
        print("ERROR: requests + beautifulsoup4 are required. Run: pip install requests beautifulsoup4")
        return

    paths.data_raw.mkdir(parents=True, exist_ok=True)
    session = requests_module.Session()
    existing_rows = load_existing_rows(output_path)
    consecutive_request_errors = 0
    aborted_early = False

    if not dry_run and preflight_url:
        preflight_headers = {
            "User-Agent": random.choice(user_agents),
            "Accept-Language": accept_language,
            "Accept": "text/html,application/xhtml+xml",
        }
        try:
            preflight_resp = session.get(
                preflight_url,
                headers=preflight_headers,
                timeout=preflight_timeout_seconds,
            )
            if preflight_resp.status_code >= 500 and reuse_existing_on_failure and existing_rows:
                log.warning(
                    "Matsumoto preflight returned server error "
                    f"{preflight_resp.status_code}. Reusing existing output rows: {len(existing_rows)}"
                )
                return
        except request_exception as exc:
            if reuse_existing_on_failure and existing_rows:
                log.warning(
                    "Matsumoto preflight failed; reusing existing output rows "
                    f"({len(existing_rows)}). Details: {exc}"
                )
                return

    labeler = ModelGenderLabeler(
        model_name=model_name,
        cache_path=paths.data_raw / "gender_model_cache.json",
        threshold=model_threshold,
    )
    results = []

    log.info(f"Matsumoto Kiyoshi scrape — {len(target_products)} products "
             f"{'[DRY RUN]' if dry_run else ''}")

    for i, product in enumerate(target_products):
        row = scrape_product(
            product=product,
            session=session,
            labeler=labeler,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            dry_run=dry_run,
        )
        results.append(row)

        if row.get("scrape_status") == "REQUEST_ERROR":
            consecutive_request_errors += 1
        else:
            consecutive_request_errors = 0

        if (
            not dry_run
            and abort_after_consecutive_request_errors > 0
            and consecutive_request_errors >= abort_after_consecutive_request_errors
        ):
            log.error(
                "Aborting Matsumoto scrape early after repeated request errors "
                f"({consecutive_request_errors} consecutive failures)."
            )
            aborted_early = True
            break

        if not dry_run:
            delay = random.uniform(1.5, 3.5)
            time.sleep(delay)

    if aborted_early and reuse_existing_on_failure and existing_rows:
        log.warning(
            "Reusing existing matsumoto_raw.csv because current run hit repeated request errors. "
            f"Kept previous rows: {len(existing_rows)}"
        )
        labeler.persist()
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    labeler.persist()

    ok   = sum(1 for r in results if r["scrape_status"] == "OK")
    fail = len(results) - ok
    log.info(f"\nDone. OK={ok}  Failed={fail}  Written → {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-name", default=default_model_name)
    parser.add_argument("--model-threshold", default=default_model_threshold, type=float)
    parser.add_argument("--timeout-seconds", default=default_timeout_seconds, type=float)
    parser.add_argument("--max-retries", default=default_max_retries, type=int)
    parser.add_argument("--retry-backoff-seconds", default=default_retry_backoff_seconds, type=float)
    parser.add_argument("--retry-jitter-seconds", default=default_retry_jitter_seconds, type=float)
    parser.add_argument(
        "--abort-after-consecutive-request-errors",
        default=default_abort_after_consecutive_request_errors,
        type=int,
    )
    parser.add_argument(
        "--reuse-existing-on-failure",
        action=argparse.BooleanOptionalAction,
        default=default_reuse_existing_on_failure,
        help="Reuse previous matsumoto_raw.csv if run aborts early from repeated request errors.",
    )
    args = parser.parse_args()

    main(
        dry_run=args.dry_run,
        model_name=args.model_name,
        model_threshold=args.model_threshold,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        retry_jitter_seconds=args.retry_jitter_seconds,
        abort_after_consecutive_request_errors=args.abort_after_consecutive_request_errors,
        reuse_existing_on_failure=args.reuse_existing_on_failure,
    )
