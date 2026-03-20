"""
Loads all 150 Hyderabad pairs from data/clean/pink_tax_pairs.csv.

BigBasket and Blinkit are both React apps so prices are injected by JS after
page load, so a real browser (Selenium headless Chrome) is required.

Features:
  • CSV-driven, no hardcoded product list, always in sync with dataset
  • Native BigBasket/Blinkit search only (no external fallback search engine)
  • URL JSON cache survives restarts, avoids repeat searches
  • Resume mode, skips already-OK rows
  • Debug screenshots on PRICE_NOT_FOUND so you can fix stale selectors
  • --retailer flag to run only bigbasket or only blinkit
"""

import csv, json, time, random, re, argparse, logging, sys, unicodedata
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from bs4 import BeautifulSoup  # type: ignore[import-not-found]

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.scraping_config import (
    cfg_delay,
    cfg_float,
    cfg_int,
    cfg_list,
    cfg_path,
    cfg_str,
    load_scraping_source_config,
)
from pink_tax.utils import enforce_single_window, select_diverse_pair_codes

# Selenium imports are optional since scraper can run in non-browser mode with reduced success.
selenium_ok = False

try:
    from selenium import webdriver  # type: ignore[import-not-found]
    from selenium.webdriver.chrome.options import Options  # type: ignore[import-not-found]
    from selenium.webdriver.common.by import By  # type: ignore[import-not-found]
    from selenium.webdriver.support.ui import WebDriverWait  # type: ignore[import-not-found]
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore[import-not-found]
    from selenium.common.exceptions import (  # type: ignore[import-not-found]
        TimeoutException, WebDriverException
    )
    selenium_ok = True
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

pair_seed_csv = root / "data" / "spec" / "pair_seed_catalog.csv"
legacy_pairs_csv  = root / "data" / "clean" / "pink_tax_pairs.csv"
pairs_csv = pair_seed_csv if pair_seed_csv.exists() else legacy_pairs_csv
scraper_config = load_scraping_source_config(root, "bigbasket")
output_bb = cfg_path(root, scraper_config, "output_path", "data/raw/bigbasket_raw.csv")
output_bl = cfg_path(root, scraper_config, "blinkit_output_path", "data/raw/blinkit_raw.csv")
found_urls_bb = cfg_path(
    root, scraper_config, "found_urls_path", "data/raw/bigbasket_found_urls.json"
)
found_urls_bl = cfg_path(
    root, scraper_config, "blinkit_found_urls_path", "data/raw/blinkit_found_urls.json"
)
debug_dir = root / "data" / "raw" / "debug"

city = cfg_str(scraper_config, "city", "Hyderabad")
currency = cfg_str(scraper_config, "currency", "INR")
today = str(date.today())
wait_timeout = int(cfg_float(scraper_config, "wait_timeout_seconds", 14.0))
page_load_timeout = cfg_float(scraper_config, "page_load_timeout_seconds", 25.0)
page_settle = cfg_float(scraper_config, "page_settle_seconds", 2.5)
browser_wait_seconds = cfg_float(scraper_config, "browser_wait_seconds", 10.0)
driver_get_retries = cfg_int(scraper_config, "driver_get_retries", 2)
driver_get_retry_pause = cfg_delay(scraper_config, "driver_get_retry_pause", 2.0, 4.0)
driver_recycle_every = cfg_int(scraper_config, "driver_recycle_every", 30)
search_pause = cfg_delay(scraper_config, "search_delay", 2.0, 4.0)
page_delay = cfg_delay(scraper_config, "product_delay", 4.0, 8.0)
bigbasket_search_base_url = cfg_str(
    scraper_config, "search_base_url", "https://www.bigbasket.com/ps/?q={query}"
)
blinkit_search_base_url = cfg_str(
    scraper_config, "blinkit_search_base_url", "https://blinkit.com/s/?q={query}"
)
user_agent = cfg_list(
    scraper_config,
    "user_agents",
    [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ],
)[0]
allowed_brands = {
    b.strip().lower()
    for b in cfg_list(scraper_config, "allowed_brands", [])
    if b.strip()
}

fieldnames = [
    "pair_code", "city", "brand", "category", "gender_label", "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status",
]

def load_hyd_products() -> list[dict]:
    """
    Load products for Hyderabad pairs from the seed catalog CSV.
    """

    if not pairs_csv.exists():
        raise FileNotFoundError(
            f"Missing seed pairs CSV. Expected one of: "
            f"{root / 'data' / 'spec' / 'pair_seed_catalog.csv'} or "
            f"{root / 'data' / 'clean' / 'pink_tax_pairs.csv'}"
        )
    products, seen = [], set()
    skipped_brand = 0
    with open(pairs_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["city"] != city:
                continue
            pc = row["pair_code"]
            if pc in seen:
                continue
            seen.add(pc)
            brand = row["brand"]
            brand_query = brand.split("/")[0].strip()
            if allowed_brands and brand_query.lower() not in allowed_brands:
                skipped_brand += 1
                continue
            for gender, name_col, size_col in [
                ("female", "female_product", "female_size"),
                ("male",   "male_product",   "male_size"),
            ]:
                name = row[name_col]
                size = row[size_col]
                gkw  = "women" if gender == "female" else "men"
                products.append({
                    "pair_code":    pc,
                    "gender_label": gender,
                    "product_name": name,
                    "brand":        brand,
                    "category":     row["category"],
                    "size_ml_or_g": size,
                    "match_quality": row["match_quality"],
                    "search_query": build_query(name, gkw, size),
                    "brand_kw":     normalize_text(brand_query),
                    "brand_query":  brand_query,
                    "gender_hint":  gkw,
                    "category_kw":  normalize_text(row["category"]),
                })
    if skipped_brand:
        log.info(f"Brand filter active: skipped {skipped_brand} pairs outside allowed_brands")
    log.info(f"Loaded {len(products)} products from {len(seen)} pairs ({city})")
    return products

def build_query(name: str, gkw: str, size: str) -> str:
    cleaned_name = re.sub(
        r"\b(japan|japanese|tokyo|jp)\b|[日本東京女性用男性用]+",
        " ",
        str(name),
        flags=re.IGNORECASE,
    )
    words = cleaned_name.split()[:7]
    q = " ".join(words)
    try:
        sz = float(size)
        if sz > 1 and str(int(sz)) not in q:
            q += f" {int(sz)}ml" if sz < 1000 else f" {int(sz)}g"
    except (ValueError, TypeError):
        pass
    if gkw not in q.lower():
        q += f" {gkw}"
    return q

def normalize_text(text: str) -> str:
    """
    Normalize text for robust matching.
    """

    folded = unicodedata.normalize("NFKD", text)
    no_marks = "".join(ch for ch in folded if not unicodedata.combining(ch))
    low = no_marks.lower()
    return re.sub(r"[^a-z0-9]+", " ", low).strip()


def as_text(value: Any) -> str:
    """
    Convert BeautifulSoup attribute values to plain text.
    """

    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value if v is not None)
    return str(value)

def build_query_variants(
    base_query: str,
    brand_query: str,
    category_kw: str,
    gender_hint: str,
) -> list[str]:
    """
    Build progressively looser queries for same-brand retrieval.
    """

    category_hint = category_kw.split()[0] if category_kw else ""
    variants = [
        base_query,
        f"{brand_query} {category_hint} {gender_hint}".strip(),
        f"{brand_query} {gender_hint}".strip(),
        f"{brand_query} {category_hint}".strip(),
        brand_query,
    ]
    out: list[str] = []
    seen: set[str] = set()
    for raw in variants:
        q = normalize_text(raw)
        if q and q not in seen:
            out.append(q)
            seen.add(q)
    return out

def cached_chromedriver_path() -> str | None:
    """
    Return a cached chromedriver binary path if available.
    """

    home = Path.home()
    candidates = sorted(home.glob(".wdm/drivers/chromedriver/**/chromedriver"), reverse=True)
    for path in candidates:
        if path.is_file():
            return str(path)
    return None

def build_driver(headless: bool = True, user_data_dir: str = ""):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_experimental_option(
        "prefs",
        {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.popups": 0,
        },
    )
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument(f"--user-agent={user_agent}")
    from selenium.webdriver.chrome.webdriver import WebDriver as ChromeWebDriver  # type: ignore[import-not-found]
    from selenium.webdriver.chrome.service import Service as ChromeService  # type: ignore[import-not-found]

    driver = None
    try:
        cached = cached_chromedriver_path()
        if cached:
            driver = ChromeWebDriver(service=ChromeService(cached), options=opts)
        else:
            from webdriver_manager.chrome import ChromeDriverManager  # type: ignore[import-not-found]

            driver = ChromeWebDriver(
                service=ChromeService(ChromeDriverManager().install()), options=opts)
    except Exception:
        driver = ChromeWebDriver(options=opts)
    try:
        driver.set_page_load_timeout(page_load_timeout)
    except Exception:
        pass
    enforce_single_window(driver)
    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def safe_driver_get(driver, url: str, retries: int | None = None) -> bool:
    """
    Navigate with retries to reduce hard failures from transient WebDriver timeouts.
    """

    total_retries = retries if retries is not None else max(driver_get_retries, 1)
    for attempt in range(total_retries):
        try:
            enforce_single_window(driver)
            driver.get(url)
            enforce_single_window(driver)
            return True
        except Exception as exc:
            log.warning(f"  Browser GET error ({attempt+1}/{total_retries}): {exc}")
            if attempt < total_retries - 1:
                time.sleep(random.uniform(*driver_get_retry_pause))
    return False

def _collect_links_from_html(html: str, regex_pattern: str, base_url: str) -> list[str]:
    raw_html = (html or "").replace("\\/", "/")
    links: list[str] = []
    for match in re.findall(regex_pattern, raw_html):
        href = match[0] if isinstance(match, tuple) else match
        href = href.split("?")[0]
        if href.startswith("/"):
            href = f"{base_url}{href}"
        if href not in links:
            links.append(href)
    return links

def is_access_denied_page(driver) -> bool:
    """
    Detect hard block pages where no product data can be parsed.
    """

    try:
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
    except Exception:
        body_text = ""
    try:
        html_text = (driver.page_source or "").lower()
    except Exception:
        html_text = ""
    try:
        current_url = str(driver.current_url or "").lower()
    except Exception:
        current_url = ""

    text = " ".join([body_text, html_text, current_url])
    signals = [
        "access denied",
        "permission to access",
        "errors.edgesuite.net",
        "reference #",
        "akamai",
    ]
    return any(signal in text for signal in signals)

def extract_listing_price(driver, link_css: str, href_tokens: tuple[str, ...], brand_kw: str = "") -> tuple[str | None, float | None]:
    """
    Extract (url, price) from a search listing page by scanning around product links.
    """

    soup = BeautifulSoup(driver.page_source, "html.parser")
    seen: set[str] = set()
    for anchor in soup.select(link_css):
        href = as_text(anchor.get("href")).strip()
        if not href:
            continue
        if not any(token in href for token in href_tokens):
            continue
        if href.startswith("/"):
            if "blinkit.com" in driver.current_url:
                href = f"https://blinkit.com{href}"
            else:
                href = f"https://www.bigbasket.com{href}"
        href = href.split("?")[0]
        if href in seen:
            continue
        seen.add(href)

        # Gather nearby text where listing cards usually include the price.
        context_parts = [anchor.get_text(" ", strip=True)]
        node = anchor
        for _ in range(4):
            node = node.parent
            if node is None:
                break
            context_parts.append(node.get_text(" ", strip=True))
        context = " ".join(context_parts)
        norm_context = normalize_text(context)
        if brand_kw and brand_kw not in norm_context and brand_kw not in normalize_text(href):
            continue

        candidates = [parse_inr(match) for match in re.findall(r"(?:[₹]\s*[\d,]+(?:\.\d+)?)|(?:Rs\.?\s*[\d,]+(?:\.\d+)?)", context)]
        values = [value for value in candidates if value and 10 <= value <= 100000]
        if values:
            return href, min(values)
    return None, None

def search_listing_price_bigbasket(
    query: str,
    driver,
    brand_kw: str = "",
    brand_query: str = "",
    category_kw: str = "",
    gender_hint: str = "",
) -> tuple[str | None, float | None]:
    """
    Search BigBasket listing pages and extract first matching listing price.
    """

    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    for current in variants:
        search_url = bigbasket_search_base_url.format(query=quote_plus(current))
        if not safe_driver_get(driver, search_url):
            continue
        time.sleep(random.uniform(*search_pause))
        href, price = extract_listing_price(
            driver=driver,
            link_css="a[href*='/pd/']",
            href_tokens=("/pd/",),
            brand_kw=brand_kw,
        )
        if href and price is not None:
            return href, price
    return None, None

def search_listing_price_blinkit(
    query: str,
    driver,
    brand_kw: str = "",
    brand_query: str = "",
    category_kw: str = "",
    gender_hint: str = "",
) -> tuple[str | None, float | None]:
    """
    Search Blinkit listing pages and extract first matching listing price.
    """

    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    for current in variants:
        search_url = blinkit_search_base_url.format(query=quote_plus(current))
        if not safe_driver_get(driver, search_url):
            continue
        time.sleep(random.uniform(*search_pause))
        href, price = extract_listing_price(
            driver=driver,
            link_css="a[href*='/prn/'], a[href*='/p/'], a[href*='/products/']",
            href_tokens=("/prn/", "/p/", "/products/"),
            brand_kw=brand_kw,
        )
        if href and price is not None:
            return href, price
        # For listing cards that expose price text but no stable product URL, we attach the search URL.
        listing_price, _, _ = extract_price_blinkit(driver)
        if listing_price is not None:
            return search_url, listing_price
    return None, None

def search_bigbasket(
    query: str,
    driver,
    brand_kw: str = "",
    brand_query: str = "",
    category_kw: str = "",
    gender_hint: str = "",
) -> str | None:
    """
    Search BigBasket and return first matching /pd/ URL.
    """

    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    for idx, current in enumerate(variants, 1):
        search_url = bigbasket_search_base_url.format(query=quote_plus(current))
        log.info(f"  🔍 BB: {current} [{idx}/{len(variants)}]")
        try:
            if not safe_driver_get(driver, search_url):
                continue
            time.sleep(random.uniform(*search_pause))
            try:
                WebDriverWait(driver, browser_wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/pd/']"))
                )
            except TimeoutException:
                pass
            strict: list[str] = []
            loose: list[str] = []
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/pd/']"):
                href = a.get_attribute("href") or ""
                text = normalize_text(a.text)
                if "/pd/" not in href:
                    continue
                cleaned = href.split("?")[0]
                if brand_kw and brand_kw not in text and brand_kw not in normalize_text(href):
                    loose.append(cleaned)
                else:
                    strict.append(cleaned)

            if not strict:
                for href in _collect_links_from_html(
                    driver.page_source,
                    r"(?:https?://www\.bigbasket\.com/pd/\d+[^\s\"'<>]*)|(?:/pd/\d+[^\s\"'<>]*)",
                    "https://www.bigbasket.com",
                ):
                    href_norm = normalize_text(href)
                    if brand_kw and brand_kw not in href_norm:
                        loose.append(href)
                    else:
                        strict.append(href)

            if strict:
                pick = random.choice(strict[:5])
                log.info(f"  ✓ BB native strict: {pick[:70]}")
                return pick
            if loose:
                pick = random.choice(loose[:5])
                log.info(f"  ✓ BB native loose-brand: {pick[:70]}")
                return pick
        except Exception as e:
            log.warning(f"  BB search error: {e}")
    log.warning("  No BB native result")
    return None

def search_blinkit(
    query: str,
    driver,
    brand_kw: str = "",
    brand_query: str = "",
    category_kw: str = "",
    gender_hint: str = "",
) -> str | None:
    """
    Search Blinkit and return first matching product URL.
    """

    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    for idx, current in enumerate(variants, 1):
        search_url = blinkit_search_base_url.format(query=quote_plus(current))
        log.info(f"  🔍 BL: {current} [{idx}/{len(variants)}]")
        try:
            if not safe_driver_get(driver, search_url):
                continue
            time.sleep(random.uniform(*search_pause))
            try:
                WebDriverWait(driver, browser_wait_seconds).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "a[href*='/prn/'], a[href*='/p/'], a[href*='/products/'], div[class*='Product']")))
            except TimeoutException:
                pass
            strict: list[str] = []
            loose: list[str] = []
            for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/prn/'], a[href*='/p/'], a[href*='/products/']"):
                href = a.get_attribute("href") or ""
                text = normalize_text(a.text)
                if "/prn/" not in href and "/p/" not in href and "/products/" not in href:
                    continue
                cleaned = href.split("?")[0]
                if brand_kw and brand_kw not in text and brand_kw not in normalize_text(href):
                    loose.append(cleaned)
                else:
                    strict.append(cleaned)

            if not strict:
                for href in _collect_links_from_html(
                    driver.page_source,
                    r"(?:https?://blinkit\.com/(?:prn|p|products)/[^\s\"'<>]*)|(?:/(?:prn|p|products)/[^\s\"'<>]*)",
                    "https://blinkit.com",
                ):
                    href_norm = normalize_text(href)
                    if brand_kw and brand_kw not in href_norm:
                        loose.append(href)
                    else:
                        strict.append(href)

            if strict:
                pick = random.choice(strict[:5])
                log.info(f"  ✓ BL native strict: {pick[:70]}")
                return pick
            if loose:
                pick = random.choice(loose[:5])
                log.info(f"  ✓ BL native loose-brand: {pick[:70]}")
                return pick
        except Exception as e:
            log.warning(f"  BL search error: {e}")
    log.warning("  No BL native result")
    return None

bb_price_selectors = [
    "[qa='discounted-price']",
    "[qa='selling-price']",
    "span.discnt-price",
    "span.selling-price",
    "div.PriceContainer span",
    "span[class*='Price__']",
    "div[class*='price'] span",
    "span[class*='price']",
]
bb_original_selectors = [
    "[qa='mrp']",
    "span.mrp-price",
    "span.discnt-price-w-o",
    "span[class*='line-through']",
    "span[class*='MRP']",
    "del",
]

inr_re = re.compile(r"[₹,\s\xa0]")

def parse_inr(raw_text: str) -> float | None:
    clean = inr_re.sub("", raw_text or "").strip()
    if not clean:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", clean)
    if not match:
        return None
    try:
        value = float(match.group(1))
        if value > 100000:
            return value / 100.0
        return value
    except ValueError:
        return None

def scroll_page(driver, steps: int = 6, pause_seconds: float = 0.5) -> None:
    """
    Scroll through the page to trigger lazy-loaded price elements.
    """

    try:
        total_height = int(driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);") or 0)
    except Exception:
        total_height = 0
    if total_height <= 0:
        return

    for idx in range(1, steps + 1):
        try:
            y = int(total_height * idx / steps)
            driver.execute_script("window.scrollTo(0, arguments[0]);", y)
            time.sleep(pause_seconds)
        except Exception:
            break
    try:
        driver.execute_script("window.scrollTo(0, 0);")
    except Exception:
        pass

def extract_price_bigbasket(driver) -> tuple:
    price = orig = None
    promo = False

    try:
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(page_settle)
        scroll_page(driver)
    except TimeoutException:
        return None, None, False

    for sel in bb_price_selectors:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                parsed = parse_inr(el.text)
                if parsed:
                    price = parsed
                    break
            if price:
                break
        except Exception:
            continue

    for sel in bb_original_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            parsed = parse_inr(el.text)
            if parsed:
                op = parsed
                if price and op > price:
                    orig, promo = op, True
                break
        except Exception:
            continue

    if price is None:
        html = driver.page_source
        for pattern in [
            r"[₹]\s*([\d,]+(?:\.\d+)?)",
            r"Rs\.?\s*([\d,]+(?:\.\d+)?)",
            r'"discounted_price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            r'"selling_price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            r'"final_price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            r'"price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        ]:
            match = re.search(pattern, html)
            if not match:
                continue
            parsed = parse_inr(match.group(1))
            if parsed and parsed >= 10:
                price = parsed
                break

    if price is None:
        text = driver.find_element(By.TAG_NAME, "body").text
        candidates = [parse_inr(m) for m in re.findall(r"(?:[₹]\s*[\d,]+(?:\.\d+)?)|(?:Rs\.?\s*[\d,]+(?:\.\d+)?)", text)]
        values = [v for v in candidates if v and 10 <= v <= 100000]
        if values:
            price = min(values)

    return price, orig, promo

bl_price_selectors = [
    "[data-testid='product-price']",
    "div[class*='ProductVariants__PriceContainer'] span",
    "span[class*='Price__StyledPrice']",
    "div[class*='product-price']",
    "div.tw-flex span[class*='font-bold']",
    "span[class*='price']",
]
bl_original_selectors = [
    "span[class*='line-through']",
    "span[class*='StrikePrice']",
    "span[class*='mrp']",
    "del",
]

def extract_price_blinkit(driver) -> tuple:
    price = orig = None
    promo = False

    try:
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(page_settle)
        scroll_page(driver)
    except TimeoutException:
        return None, None, False

    for sel in bl_price_selectors:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                parsed = parse_inr(el.text)
                if parsed:
                    price = parsed
                    break
            if price:
                break
        except Exception:
            continue

    for sel in bl_original_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            parsed = parse_inr(el.text)
            if parsed:
                op = parsed
                if price and op > price:
                    orig, promo = op, True
                break
        except Exception:
            continue

    if price is None:
        html = driver.page_source
        for pattern in [
            r"[₹]\s*([\d,]+(?:\.\d+)?)",
            r"Rs\.?\s*([\d,]+(?:\.\d+)?)",
            r'"price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            r'"mrp"\s*:\s*"?(\d+(?:\.\d+)?)"?',
            r'"selling_price"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        ]:
            match = re.search(pattern, html)
            if not match:
                continue
            parsed = parse_inr(match.group(1))
            if parsed and parsed >= 10:
                price = parsed
                break

    if price is None:
        text = driver.find_element(By.TAG_NAME, "body").text
        candidates = [parse_inr(m) for m in re.findall(r"(?:[₹]\s*[\d,]+(?:\.\d+)?)|(?:Rs\.?\s*[\d,]+(?:\.\d+)?)", text)]
        values = [v for v in candidates if v and 10 <= v <= 100000]
        if values:
            price = min(values)

    return price, orig, promo

def scrape_one(product: dict, driver, url_cache: dict,
               retailer_name: str, search_fn, extract_fn,
               dry_run: bool = False, skip_search: bool = False) -> dict:
    row = {
        "pair_code":            product["pair_code"],
        "city":                 city,
        "brand":                product["brand"],
        "category":             product["category"],
        "gender_label":         product["gender_label"],
        "product_name":         product["product_name"],
        "size_ml_or_g":         product["size_ml_or_g"],
        "price_local":          None,
        "currency":             currency,
        "original_price_local": None,
        "on_promotion":         False,
        "retailer":             retailer_name,
        "match_quality":        product["match_quality"],
        "confidence":           "LOW",
        "date_scraped":         today,
        "source_url":           "",
        "scrape_status":        "PENDING",
    }

    if dry_run:
        row["scrape_status"] = "DRY_RUN"
        log.info(f"  [DRY] {retailer_name:<12} {product['gender_label']:<6} {product['product_name']}")
        return row

    ck  = f"{product['pair_code']}|{product['gender_label']}"
    url = url_cache.get(ck)

    if not url and not skip_search:
        url = search_fn(
            product["search_query"],
            driver,
            product["brand_kw"],
            product.get("brand_query", ""),
            product.get("category_kw", ""),
            product.get("gender_hint", ""),
        )
        time.sleep(random.uniform(*search_pause))

    if not url and retailer_name == "Blinkit" and not skip_search:
        fallback_url, fallback_price = search_listing_price_blinkit(
            product["search_query"],
            driver,
            product["brand_kw"],
            product.get("brand_query", ""),
            product.get("category_kw", ""),
            product.get("gender_hint", ""),
        )
        if fallback_url and fallback_price is None:
            url = fallback_url
        elif fallback_price is not None:
            row.update({
                "source_url": fallback_url or "",
                "price_local": fallback_price,
                "original_price_local": None,
                "on_promotion": False,
                "confidence": "MED",
                "scrape_status": "OK",
            })
            log.info(
                f"  ✓ {retailer_name:<12} {product['gender_label']:<6} "
                f"{product['product_name'][:48]:<48} ₹{fallback_price:.0f}  (listing)"
            )
            return row

    if not url:
        row["scrape_status"] = "URL_NOT_FOUND"
        log.warning(f"  ✗ {retailer_name}: No URL for {product['product_name']}")
        return row

    row["source_url"] = url
    url_cache[ck]     = url

    try:
        if not safe_driver_get(driver, url):
            row["scrape_status"] = "REQUEST_ERROR"
            return row
        price, orig, promo = extract_fn(driver)

        if price is None:
            blocked = is_access_denied_page(driver)
            if blocked:
                row["scrape_status"] = "BLOCKED"
                log.warning(f"  ✗ {retailer_name}: Access blocked, {product['product_name']}")
                debug_dir.mkdir(parents=True, exist_ok=True)
                fname = f"{product['pair_code']}_{product['gender_label']}_{retailer_name.replace(' ','_')}.png"
                try:
                    driver.save_screenshot(str(debug_dir / fname))
                    log.info(f"    Screenshot → {debug_dir / fname}")
                except Exception:
                    pass
                return row

            # Fallback: listing-level price extraction from native search page.
            if retailer_name == "BigBasket":
                fallback_url, fallback_price = search_listing_price_bigbasket(
                    product["search_query"],
                    driver,
                    product["brand_kw"],
                    product.get("brand_query", ""),
                    product.get("category_kw", ""),
                    product.get("gender_hint", ""),
                )
            else:
                fallback_url, fallback_price = search_listing_price_blinkit(
                    product["search_query"],
                    driver,
                    product["brand_kw"],
                    product.get("brand_query", ""),
                    product.get("category_kw", ""),
                    product.get("gender_hint", ""),
                )

            if fallback_price is not None:
                row.update({
                    "source_url": fallback_url or row["source_url"],
                    "price_local": fallback_price,
                    "original_price_local": None,
                    "on_promotion": False,
                    "confidence": "MED",
                    "scrape_status": "OK",
                })
                log.info(
                    f"  ✓ {retailer_name:<12} {product['gender_label']:<6} "
                    f"{product['product_name'][:48]:<48} ₹{fallback_price:.0f}  (listing)"
                )
            else:
                row["scrape_status"] = "PRICE_NOT_FOUND"
                log.warning(f"  ✗ {retailer_name}: No price, {product['product_name']}")
                debug_dir.mkdir(parents=True, exist_ok=True)
                fname = f"{product['pair_code']}_{product['gender_label']}_{retailer_name.replace(' ','_')}.png"
                try:
                    driver.save_screenshot(str(debug_dir / fname))
                    log.info(f"    Screenshot → {debug_dir / fname}")
                except Exception:
                    pass
        else:
            row.update({
                "price_local": price, "original_price_local": orig,
                "on_promotion": promo, "confidence": "HIGH", "scrape_status": "OK",
            })
            log.info(f"  ✓ {retailer_name:<12} {product['gender_label']:<6} "
                     f"{product['product_name'][:48]:<48} ₹{price:.0f}"
                     + ("  🏷" if promo else ""))

    except Exception as e:
        log.error(f"  ✗ {retailer_name}: scraper error, {e}")
        row["scrape_status"] = "SCRAPER_ERROR"

    return row

def main(
    dry_run=False,
    skip_search=False,
    resume=False,
    retailer_filter="both",
    target_pair=None,
    limit=None,
    browser_mode=False,
    headful=False,
    user_data_dir="",
):
    if not dry_run and not selenium_ok:
        print("ERROR: selenium not installed. Run: pip install selenium webdriver-manager")
        return
    if not dry_run and not browser_mode:
        log.warning("Browser mode is required for BigBasket/Blinkit. Enabling automatically.")
        browser_mode = True

    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)

    products = load_hyd_products()
    if target_pair:
        products = [p for p in products if p["pair_code"] == target_pair]
        if not products:
            log.error(f"Pair not found: {target_pair}"); return

    if limit:
        keep = select_diverse_pair_codes(products, limit)
        products = [p for p in products if p["pair_code"] in keep]
        log.info(f"Limit: {len(keep)} pairs (diverse) → {len(products)} products")

    run_bb = retailer_filter in ("both", "bigbasket")
    run_bl = retailer_filter in ("both", "blinkit")

    # Load URL caches
    bb_cache: dict = {}
    bl_cache: dict = {}
    if run_bb and found_urls_bb.exists():
        try:
            bb_cache = json.loads(found_urls_bb.read_text(encoding="utf-8"))
            log.info(f"BB URL cache: {len(bb_cache)} entries")
        except Exception:
            pass
    if run_bl and found_urls_bl.exists():
        try:
            bl_cache = json.loads(found_urls_bl.read_text(encoding="utf-8"))
            log.info(f"BL URL cache: {len(bl_cache)} entries")
        except Exception:
            pass

    bb_done, bl_done = set(), set()
    bb_rows, bl_rows = [], []

    if resume:
        for path, done_set, rows in [(output_bb, bb_done, bb_rows),
                                     (output_bl, bl_done, bl_rows)]:
            if path.exists():
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if row.get("scrape_status") == "OK":
                            done_set.add(f"{row['pair_code']}|{row['gender_label']}")
                            rows.append(row)
        if bb_done:
            log.info(f"BB resume: {len(bb_done)} already-OK")
        if bl_done:
            log.info(f"BL resume: {len(bl_done)} already-OK")

    driver = None
    try:
        def recycle_driver(reason: str) -> None:
            nonlocal driver
            if dry_run or not browser_mode:
                return
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass
            log.info(f"Recycling Chrome session: {reason}")
            driver = build_driver(headless=not headful, user_data_dir=user_data_dir)

        if not dry_run and browser_mode:
            log.info("Launching Chrome...")
            driver = build_driver(headless=not headful, user_data_dir=user_data_dir)
            log.info("Chrome ready.\n")

        n = len(products)
        log.info(f"\n{'='*62}")
        log.info(f"  BigBasket/Blinkit  |  {n} products  |  {'DRY RUN' if dry_run else 'LIVE'}")
        log.info(f"{'='*62}\n")

        for i, product in enumerate(products, 1):
            if (
                not dry_run
                and browser_mode
                and driver_recycle_every > 0
                and i > 1
                and (i - 1) % driver_recycle_every == 0
            ):
                recycle_driver(f"processed {i - 1} products")

            ck = f"{product['pair_code']}|{product['gender_label']}"
            log.info(f"[{i:>3}/{n}] {product['pair_code']} / {product['gender_label']}")

            def error_row(retailer_name: str, status: str, confidence: str = "0.0") -> dict:
                return {
                    "pair_code": product["pair_code"],
                    "city": city,
                    "brand": product["brand"],
                    "category": product["category"],
                    "gender_label": product["gender_label"],
                    "product_name": product["product_name"],
                    "size_ml_or_g": product["size_ml_or_g"],
                    "price_local": "",
                    "currency": currency,
                    "original_price_local": "",
                    "on_promotion": "",
                    "retailer": retailer_name,
                    "match_quality": product["match_quality"],
                    "confidence": confidence,
                    "date_scraped": today,
                    "source_url": "",
                    "scrape_status": status,
                }

            if run_bb:
                if resume and ck in bb_done:
                    log.info(f"  BB: already-OK, skipping")
                else:
                    try:
                        r = scrape_one(product, driver, bb_cache,
                                       "BigBasket", search_bigbasket, extract_price_bigbasket,
                                       dry_run=dry_run, skip_search=skip_search)
                    except Exception as exc:
                        log.error(f"  ✗ BigBasket: unexpected error, {exc}")
                        r = error_row("BigBasket", "UNEXPECTED_ERROR")
                    bb_rows.append(r)
                    if r.get("scrape_status") in {"REQUEST_ERROR", "SCRAPER_ERROR", "UNEXPECTED_ERROR"}:
                        recycle_driver(f"BigBasket status={r.get('scrape_status')}")
                    # Save cache after each product
                    found_urls_bb.write_text(
                        json.dumps(bb_cache, ensure_ascii=False, indent=2), encoding="utf-8")

            if run_bl:
                if resume and ck in bl_done:
                    log.info(f"  BL: already-OK, skipping")
                else:
                    if not dry_run and run_bb:
                        time.sleep(random.uniform(1.5, 3.0))
                    try:
                        r = scrape_one(product, driver, bl_cache,
                                       "Blinkit", search_blinkit, extract_price_blinkit,
                                       dry_run=dry_run, skip_search=skip_search)
                    except Exception as exc:
                        log.error(f"  ✗ Blinkit: unexpected error, {exc}")
                        r = error_row("Blinkit", "UNEXPECTED_ERROR")
                    bl_rows.append(r)
                    if r.get("scrape_status") in {"REQUEST_ERROR", "SCRAPER_ERROR", "UNEXPECTED_ERROR"}:
                        recycle_driver(f"Blinkit status={r.get('scrape_status')}")
                    found_urls_bl.write_text(
                        json.dumps(bl_cache, ensure_ascii=False, indent=2), encoding="utf-8")

            if not dry_run:
                time.sleep(random.uniform(*page_delay))

        def write(rows, path):
            if not rows:
                return
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader(); w.writerows(rows)
            ok = sum(1 for r in rows if r.get("scrape_status") == "OK")
            log.info(f"  {path.name}: OK={ok}/{len(rows)}")

        log.info(f"\n{'='*62}")
        if run_bb:
            write(bb_rows, output_bb)
        if run_bl:
            write(bl_rows, output_bl)
        log.info(f"{'='*62}")

    finally:
        if driver:
            driver.quit()
            log.info("Chrome closed.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run",    action="store_true")
    ap.add_argument("--no-search",  action="store_true")
    ap.add_argument("--resume",     action="store_true")
    ap.add_argument("--retailer",   choices=["both", "bigbasket", "blinkit"], default="both")
    ap.add_argument("--pair",       metavar="PAIR_CODE")
    ap.add_argument("--limit",      type=int, metavar="N",
                   help="Max number of PAIRS to scrape (products = 2×N per retailer).")
    ap.add_argument("--browser-mode", action="store_true",
                   help="Use Selenium browser mode for search + product fetch.")
    ap.add_argument("--headful", action="store_true",
                   help="Run browser mode with visible Chrome window.")
    ap.add_argument("--user-data-dir", default="",
                   help="Chrome profile directory for persistent cookies in browser mode.")
    args = ap.parse_args()
    main(dry_run=args.dry_run, skip_search=args.no_search, resume=args.resume,
         retailer_filter=args.retailer, target_pair=args.pair, limit=args.limit,
         browser_mode=args.browser_mode, headful=args.headful, user_data_dir=args.user_data_dir)
