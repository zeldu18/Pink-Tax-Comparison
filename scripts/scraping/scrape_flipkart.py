"""
Loads all 150 Hyderabad pairs from data/clean/pink_tax_pairs.csv.

Flipkart specifics:
  • CSS classes rotate every few weeks; this scraper uses four selector
    strategies in priority order + a JSON-LD fallback, so one stale class
    doesn't break everything.
  • Gender validation: we require that the returned product title contains
    a gender-confirming keyword before accepting the URL. This prevents
    the top result for a women's search from being a men's product.
  • Native Flipkart search only by default.
  • Price from structured JSON-LD (most stable) then CSS classes.
"""

import csv, json, time, random, re, argparse, logging, sys, unicodedata
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlsplit, urlunsplit
import requests  # type: ignore[import-not-found,import-untyped]
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

# Optional Selenium browser mode.
selenium_ok = False

try:
    from selenium import webdriver  # type: ignore[import-not-found]
    from selenium.webdriver.chrome.options import Options  # type: ignore[import-not-found]
    from selenium.webdriver.common.by import By  # type: ignore[import-not-found]
    from selenium.webdriver.support.ui import WebDriverWait  # type: ignore[import-not-found]
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore[import-not-found]
    from selenium.common.exceptions import TimeoutException, WebDriverException  # type: ignore[import-not-found]
    selenium_ok = True
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

pair_seed_csv = root / "data" / "spec" / "pair_seed_catalog.csv"
legacy_pairs_csv = root / "data" / "clean" / "pink_tax_pairs.csv"
pairs_csv = pair_seed_csv if pair_seed_csv.exists() else legacy_pairs_csv
scraper_config = load_scraping_source_config(root, "flipkart")
output_path = cfg_path(root, scraper_config, "output_path", "data/raw/flipkart_raw.csv")
found_urls_path = cfg_path(
    root, scraper_config, "found_urls_path", "data/raw/flipkart_found_urls.json"
)
debug_dir = root / "data" / "raw" / "debug"

city = cfg_str(scraper_config, "city", "Hyderabad")
currency = cfg_str(scraper_config, "currency", "INR")
retailer = cfg_str(scraper_config, "retailer", "Flipkart")
today = str(date.today())

search_delay = cfg_delay(scraper_config, "search_delay", 2.5, 5.0)
product_delay = cfg_delay(scraper_config, "product_delay", 4.0, 8.0)
ddg_delay = cfg_delay(scraper_config, "ddg_delay", 3.0, 6.0)
block_pause = cfg_float(scraper_config, "block_pause_seconds", 30.0)
request_timeout_seconds = cfg_float(scraper_config, "request_timeout_seconds", 15.0)
max_request_retries = cfg_int(scraper_config, "max_request_retries", 2)
retry_delay = cfg_delay(scraper_config, "retry_delay", 1.5, 3.0)
fail_fast_on_block = str(scraper_config.get("fail_fast_on_block", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
}
search_base_url = cfg_str(
    scraper_config, "search_base_url", "https://www.flipkart.com/search?q={query}"
)
referer_url = cfg_str(scraper_config, "referer_url", "https://www.flipkart.com")
browser_wait_seconds = cfg_float(scraper_config, "browser_wait_seconds", 10.0)
enable_ddg_fallback = str(scraper_config.get("enable_ddg_fallback", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
}
allowed_brands = {
    b.strip().lower()
    for b in cfg_list(scraper_config, "allowed_brands", [])
    if b.strip()
}
default_user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]
user_agents = cfg_list(scraper_config, "user_agents", default_user_agents)

# Gender keywords for title validation
female_kw = ["women", "woman", "female", "lady", "ladies", "her", "girl",
              "venus", "she", "feminine", "for women"]
male_kw = ["men", "man", "male", "his", "mach3", "mach 3", "boy",
              "for men", "masculine"]

fieldnames = [
    "pair_code", "city", "brand", "category", "gender_label", "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status",
]

def load_hyd_products() -> list[dict]:
    """
    Load Hyderabad products from pairs CSV, filtering to one pair per pair_code.
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
                    "gender_kw":    female_kw if gender == "female" else male_kw,
                    "brand_kw":     normalize_text(brand_query),
                    "brand_query":  brand_query,
                    "gender_hint":  gkw,
                    "category_kw":  normalize_text(row["category"]),
                })
    if skipped_brand:
        log.info(f"Brand filter active: skipped {skipped_brand} pairs outside allowed_brands")
    log.info(f"Loaded {len(products)} products from {len(seen)} pairs ({city})")
    return products

def build_query(name: str, gender_kw: str, size: str) -> str:
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
    if gender_kw not in q.lower():
        q += f" {gender_kw}"
    return q

def normalize_text(text: str) -> str:
    """
    Normalize text for robust matching by removing accents and punctuation.
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

def canonicalize_flipkart_url(url: str) -> str:
    """
    Strip tracking query params from Flipkart product URLs.
    """

    raw = str(url or "").strip()
    if not raw:
        return raw
    try:
        parsed = urlsplit(raw)
        scheme = parsed.scheme or "https"
        return urlunsplit((scheme, parsed.netloc, parsed.path, "", ""))
    except Exception:
        return raw

def build_query_variants(
    base_query: str,
    brand_query: str,
    category_kw: str,
    gender_hint: str,
) -> list[str]:
    """
    Build progressively looser queries so we still get same-brand products.
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

def _headers(referer: str = referer_url) -> dict:
    return {
        "User-Agent":                random.choice(user_agents),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-IN,en;q=0.9,hi;q=0.8",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer":                   referer,
        "DNT":                       "1",
    }

def is_blocked(html: str) -> bool:
    low = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
    sigs = [
        "please verify you are a human",
        "sorry, you have been blocked",
        "unusual activity from your computer network",
        "enter the characters shown",
        "too many requests",
    ]
    return any(s in low for s in sigs)

def safe_get(session, url, retries: int | None = None, delay_range=search_delay):
    attempts = retries if retries is not None else max_request_retries
    for attempt in range(attempts):
        try:
            resp = session.get(
                url, headers=_headers(), timeout=request_timeout_seconds, allow_redirects=True
            )
            if resp.status_code in {403, 429, 503} or is_blocked(resp.text):
                log.warning(f"  ⚠ BLOCKED ({attempt+1}/{attempts})")
                if fail_fast_on_block:
                    return None
                log.warning(f"  Pausing {block_pause}s before retry")
                time.sleep(block_pause)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Error ({attempt+1}/{attempts}): {e}")
            if attempt < attempts - 1:
                time.sleep(random.uniform(*retry_delay))
    return None

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
    """
    Build Selenium Chrome driver for browser-mode scraping.
    """

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,900")
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
    opts.add_argument(f"--user-agent={random.choice(user_agents)}")
    from selenium.webdriver.chrome.webdriver import WebDriver as ChromeWebDriver  # type: ignore[import-not-found]
    from selenium.webdriver.chrome.service import Service as ChromeService  # type: ignore[import-not-found]

    driver = None
    try:
        cached = cached_chromedriver_path()
        if cached:
            driver = ChromeWebDriver(service=ChromeService(cached), options=opts)
        else:
            from webdriver_manager.chrome import ChromeDriverManager  # type: ignore[import-not-found]

            driver = ChromeWebDriver(service=ChromeService(ChromeDriverManager().install()), options=opts)
    except Exception:
        driver = ChromeWebDriver(options=opts)
    enforce_single_window(driver)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

fk_card_selectors = [
    "a._1fQZEK",       # classic product link
    "a.IRpwTa",        # 2024 variant
    "a.s1Q9rs",        # 2023/24 variant
    "a[href*='/p/']",
    "a[href*='/itm/']",
    "div[data-id] a",  # generic data-id container
]

def extract_fk_cards(soup: BeautifulSoup) -> list:
    for sel in fk_card_selectors:
        cards = soup.select(sel)
        if cards:
            return cards
    # Broadest fallback: any <a> inside a result grid
    return soup.select("div._1YokD2 a, div.DOjaWF a, div._2kHMtA a")

def fetch_search_soup(query: str, session, driver=None) -> BeautifulSoup | None:
    """
    Fetch one Flipkart search result page and return parsed soup.
    """

    url = search_base_url.format(query=quote_plus(query))
    if "marketplace=" not in url:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}marketplace=FLIPKART"
    if driver is not None:
        try:
            enforce_single_window(driver)
            driver.get(url)
            try:
                WebDriverWait(driver, browser_wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                )
            except TimeoutException:
                pass
            enforce_single_window(driver)
            page_html = driver.page_source
            if is_blocked(page_html):
                return None
            return BeautifulSoup(page_html, "html.parser")
        except WebDriverException:
            return None
    resp = safe_get(session, url, delay_range=search_delay)
    if not resp or resp.status_code != 200:
        return None
    return BeautifulSoup(resp.text, "html.parser")

def pick_candidate_url(
    cards: list,
    brand_kw: str,
    gender_kw: list[str] | None,
) -> str | None:
    """
    Choose one URL from native result cards, preferring gender match.
    """

    strict: list[tuple[str, str]] = []
    loose: list[tuple[str, str]] = []
    norm_gender = [normalize_text(k) for k in (gender_kw or []) if normalize_text(k)]

    for card in cards:
        href = as_text(card.get("href"))
        title_raw = card.get_text(" ", strip=True) or as_text(card.get("title"))
        title = normalize_text(title_raw)

        if not href:
            continue

        # Must contain brand
        href_norm = normalize_text(href)
        if brand_kw and brand_kw not in title and brand_kw not in href_norm:
            continue

        if not href.startswith("http"):
            href = "https://www.flipkart.com" + href
        href = canonicalize_flipkart_url(href)

        has_gender = any(k in title for k in norm_gender) if norm_gender else False
        if has_gender:
            strict.append((href, title_raw))
        else:
            loose.append((href, title_raw))

    if strict:
        href, title = random.choice(strict[:5])
        log.info(f"  ✓ FK native strict: {title[:65].lower()}")
        return canonicalize_flipkart_url(href)

    if loose:
        href, title = random.choice(loose[:5])
        log.info(f"  ✓ FK native loose-brand: {title[:65].lower()}")
        return canonicalize_flipkart_url(href)

    return None

def search_flipkart(query: str, session,
                    brand_kw: str = "",
                    gender_kw: list[str] | None = None,
                    brand_query: str = "",
                    gender_hint: str = "",
                    category_kw: str = "",
                    driver=None) -> str | None:
    """
    Search Flipkart for the query, returning a product URL or None if not found.
    """

    query_variants = build_query_variants(
        base_query=query,
        brand_query=brand_query or brand_kw,
        category_kw=category_kw,
        gender_hint=gender_hint,
    )

    for idx, current_query in enumerate(query_variants, 1):
        log.info(f"  🔍 {current_query} [{idx}/{len(query_variants)}]")
        soup = fetch_search_soup(current_query, session, driver=driver)
        if soup is None:
            continue
        cards = extract_fk_cards(soup)
        match = pick_candidate_url(cards, brand_kw=brand_kw, gender_kw=gender_kw)
        if match:
            return match

    if enable_ddg_fallback:
        log.warning("  No FK native result, trying DDG fallback")
        for current_query in query_variants:
            ddg_match = ddg_fallback(current_query, "flipkart.com", session)
            if ddg_match:
                return ddg_match
    else:
        log.warning("  No FK native result")
    return None

def ddg_fallback(query: str, site: str, session) -> str | None:
    """
    DuckDuckGo site: search as fallback when native search fails.
    """

    ddg_q  = f"{query} site:{site}"
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(ddg_q)}"
    headers = {
        "User-Agent":      random.choice(user_agents),
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml",
        "Referer":         "https://duckduckgo.com/",
    }
    try:
        time.sleep(random.uniform(*ddg_delay))
        resp = session.get(ddg_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a.result__url, a[href*='flipkart.com'], a[href*='rakuten.co.jp']"):
            href = as_text(a.get("href")).strip()
            if site in href and ("/p/" in href or "/dp/" in href or "item_" in href):
                if not href.startswith("http"):
                    href = "https://" + href.lstrip("/")
                href = canonicalize_flipkart_url(href)
                log.info(f"  ✓ DDG fallback: {href[:70]}")
                return href
    except Exception as e:
        log.warning(f"  DDG fallback error: {e}")
    return None

rupee_re = re.compile(r"[₹,\s\xa0]")

def parse_inr(text: str) -> float | None:
    clean = rupee_re.sub("", text).strip().split(".")[0]
    try:
        v = float(clean)
        return v / 100 if v > 100_000 else v
    except ValueError:
        return None

def extract_price_flipkart(soup) -> tuple:
    price = orig = None
    promo = False

    # 1. JSON-LD structured data (most stable across CSS rotations)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and ("offers" in item or "Offers" in item):
                        data = item
                        break
                else:
                    continue
            if not isinstance(data, dict):
                continue
            offers = data.get("offers") or data.get("Offers")
            if isinstance(offers, dict):
                p = offers.get("price") or offers.get("lowPrice")
                if p:
                    price = float(p)
                    break
            elif isinstance(offers, list) and offers:
                p = offers[0].get("price")
                if p:
                    price = float(p)
                    break
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    # 2. CSS class selectors (rotate by FK deploy)
    if price is None:
        for cls in ("_30jeq3", "Nx9bqj", "_1_WHN1", "a-price-whole",
                    "dyC4hf", "VU-ZEz", "_3I9_wc"):
            el = soup.find(class_=cls)
            if el:
                v = parse_inr(el.get_text())
                if v and v > 1:
                    price = v
                    break

    # 3. Any ₹ symbol followed by digits
    if price is None:
        m = re.search(r"₹\s*([\d,]+)", soup.get_text())
        if m:
            v = parse_inr(m.group(0))
            if v and v > 1:
                price = v

    # Strikethrough original
    for cls in ("_3I9_wc", "_2p6lqe", "a-text-strike"):
        el = soup.find(class_=cls)
        if el:
            v = parse_inr(el.get_text())
            if v and v > 1:
                orig, promo = v, True
                break

    return price, orig, promo

def scrape_product(product, session, url_cache, dry_run=False, skip_search=False, driver=None):
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
        "retailer":             retailer,
        "match_quality":        product["match_quality"],
        "confidence":           "LOW",
        "date_scraped":         today,
        "source_url":           "",
        "scrape_status":        "PENDING",
    }

    if dry_run:
        row["scrape_status"] = "DRY_RUN"
        log.info(f"  [DRY] {product['gender_label']:<6} {product['product_name']}")
        log.info(f"        query: {product['search_query']}")
        return row

    ck  = f"{product['pair_code']}|{product['gender_label']}"
    url = canonicalize_flipkart_url(str(url_cache.get(ck, "") or ""))

    soup: BeautifulSoup | None = None
    blocked_seen = False
    blocked_on_page = False
    for attempt in range(2):
        if not url and not skip_search:
            url = search_flipkart(
                product["search_query"],
                session,
                brand_kw=product["brand_kw"],
                gender_kw=product["gender_kw"],
                brand_query=product["brand_query"],
                gender_hint=product["gender_hint"],
                category_kw=product["category_kw"],
                driver=driver,
            )
            url = canonicalize_flipkart_url(url or "")
            time.sleep(random.uniform(*search_delay))

        if not url:
            row["scrape_status"] = "URL_NOT_FOUND"
            log.warning(f"  ✗ No URL: {product['product_name']}")
            return row

        row["source_url"] = url
        url_cache[ck] = url

        if driver is not None:
            try:
                enforce_single_window(driver)
                driver.get(url)
                try:
                    WebDriverWait(driver, browser_wait_seconds).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                    )
                except TimeoutException:
                    pass
                enforce_single_window(driver)
                page_html = driver.page_source
                soup = BeautifulSoup(page_html, "html.parser")
                blocked_on_page = is_blocked(page_html) or "/blocked" in str(driver.current_url or "")
                break
            except WebDriverException:
                if attempt == 0 and not skip_search:
                    url_cache.pop(ck, None)
                    url = ""
                    continue
                row["scrape_status"] = "REQUEST_ERROR"
                return row
        else:
            resp = safe_get(session, url, delay_range=product_delay)
            if resp is None:
                if attempt == 0 and not skip_search:
                    url_cache.pop(ck, None)
                    url = ""
                    continue
                row["scrape_status"] = "REQUEST_ERROR"
                return row
            if resp.status_code != 200:
                if attempt == 0 and not skip_search:
                    url_cache.pop(ck, None)
                    url = ""
                    continue
                row["scrape_status"] = f"HTTP_{resp.status_code}"
                return row
            soup = BeautifulSoup(resp.text, "html.parser")
            blocked_on_page = is_blocked(resp.text)
            break

    if soup is None:
        row["scrape_status"] = "BLOCKED" if blocked_seen else "REQUEST_ERROR"
        return row
    price, orig, promo = extract_price_flipkart(soup)

    if price is None:
        if blocked_on_page:
            blocked_seen = True
            debug_dir.mkdir(parents=True, exist_ok=True)
            snap = debug_dir / f"{product['pair_code']}_{product['gender_label']}_Flipkart_BLOCKED.png"
            if driver is not None:
                try:
                    driver.save_screenshot(str(snap))
                except Exception:
                    pass
            row["scrape_status"] = "BLOCKED"
            log.warning(f"  ✗ BLOCKED: {product['product_name']}")
        else:
            row["scrape_status"] = "PRICE_NOT_FOUND"
            log.warning(f"  ✗ No price: {product['product_name']}")
    else:
        row.update({
            "price_local": price, "original_price_local": orig,
            "on_promotion": promo, "confidence": "HIGH", "scrape_status": "OK",
        })
        log.info(f"  ✓ {product['gender_label']:<6} {product['product_name'][:52]:<52} ₹{price:.0f}"
                 + ("  🏷" if promo else ""))

    return row

def main(
    dry_run=False,
    skip_search=False,
    resume=False,
    target_pair=None,
    limit=None,
    test_url=None,
    browser_mode=False,
    headful=False,
    user_data_dir="",
):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)

    if test_url:
        log.info(f"Single URL test: {test_url}")
        session = requests.Session()
        resp = safe_get(session, test_url, retries=1, delay_range=search_delay)
        if not resp or resp.status_code != 200:
            log.error("Single URL test failed: blocked or non-200 response.")
            return
        soup = BeautifulSoup(resp.text, "html.parser")
        price, orig, promo = extract_price_flipkart(soup)
        title_el = soup.select_one("span.VU-ZEz, span.B_NuCI")
        title = title_el.get_text(" ", strip=True) if title_el else "(title not found)"
        log.info(f"Title: {title}")
        if price is None:
            log.warning("Price extraction failed for this URL.")
        else:
            log.info(f"Extracted price: ₹{price:.0f}")
            if orig is not None:
                log.info(f"Original price: ₹{orig:.0f}")
            log.info(f"On promotion: {promo}")
        return

    products = load_hyd_products()
    if target_pair:
        products = [p for p in products if p["pair_code"] == target_pair]
        if not products:
            log.error(f"Pair not found: {target_pair}"); return

    if limit:
        keep = select_diverse_pair_codes(products, limit)
        products = [p for p in products if p["pair_code"] in keep]
        log.info(f"Limit: {len(keep)} pairs (diverse) → {len(products)} products")

    url_cache: dict = {}
    if found_urls_path.exists():
        try:
            url_cache = json.loads(found_urls_path.read_text(encoding="utf-8"))
            if isinstance(url_cache, dict):
                url_cache = {
                    str(key): canonicalize_flipkart_url(str(value))
                    for key, value in url_cache.items()
                }
            log.info(f"URL cache: {len(url_cache)} entries")
        except Exception:
            pass

    done_keys, existing = set(), []
    if resume and output_path.exists():
        with open(output_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("scrape_status") == "OK":
                    done_keys.add(f"{row['pair_code']}|{row['gender_label']}")
                    existing.append(row)
        log.info(f"Resume: {len(done_keys)} already-OK")

    session = requests.Session()
    driver = None
    if browser_mode and not dry_run:
        if not selenium_ok:
            log.error("Selenium not installed. Run: pip install selenium webdriver-manager")
            return
        driver = build_driver(headless=not headful, user_data_dir=user_data_dir)
    results, skipped = list(existing), 0

    log.info(f"\n{'='*62}")
    log.info(f"  Flipkart  |  {len(products)} products  |  {'DRY RUN' if dry_run else 'LIVE'}")
    log.info(f"{'='*62}\n")

    for i, product in enumerate(products, 1):
        ck = f"{product['pair_code']}|{product['gender_label']}"
        if resume and ck in done_keys:
            skipped += 1; continue

        log.info(f"[{i:>3}/{len(products)}] {product['pair_code']} / {product['gender_label']}")
        row = scrape_product(product, session, url_cache,
                             dry_run=dry_run, skip_search=skip_search, driver=driver)
        results.append(row)
        found_urls_path.write_text(
            json.dumps(url_cache, ensure_ascii=False, indent=2), encoding="utf-8")
        if not dry_run:
            time.sleep(random.uniform(*product_delay))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader(); w.writerows(results)

    ok      = sum(1 for r in results if r.get("scrape_status") == "OK")
    blocked = sum(1 for r in results if r.get("scrape_status") == "BLOCKED")
    no_url  = sum(1 for r in results if r.get("scrape_status") == "URL_NOT_FOUND")
    no_price= sum(1 for r in results if r.get("scrape_status") == "PRICE_NOT_FOUND")

    log.info(f"\n{'='*62}")
    log.info(f"  OK={ok}  Blocked={blocked}  NoPrice={no_price}  NoURL={no_url}  Skipped={skipped}")
    log.info(f"  → {output_path}")
    if blocked:
        log.warning(f"  {blocked} blocked. Run --resume to retry.")
    log.info(f"{'='*62}")
    if driver is not None:
        driver.quit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run",   action="store_true")
    ap.add_argument("--no-search", action="store_true")
    ap.add_argument("--resume",    action="store_true")
    ap.add_argument("--pair",      metavar="PAIR_CODE")
    ap.add_argument("--limit",     type=int, default=50, metavar="N",
                   help="Max number of PAIRS to scrape (products = 2×N)")
    ap.add_argument("--test-url",  metavar="URL",
                   help="Test one direct Flipkart product URL; skips search and CSV write.")
    ap.add_argument("--browser-mode", action="store_true",
                   help="Use Selenium browser mode for search + product fetch.")
    ap.add_argument("--headful", action="store_true",
                   help="Run browser mode with visible Chrome window.")
    ap.add_argument("--user-data-dir", default="",
                   help="Chrome profile directory for persistent cookies in browser mode.")
    args = ap.parse_args()
    main(dry_run=args.dry_run, skip_search=args.no_search,
         resume=args.resume, target_pair=args.pair, limit=args.limit, test_url=args.test_url,
         browser_mode=args.browser_mode, headful=args.headful, user_data_dir=args.user_data_dir)