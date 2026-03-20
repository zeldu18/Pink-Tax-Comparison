"""
Loads all 150 Tokyo pairs from data/clean/pink_tax_pairs.csv.
Japanese search queries use the product names directly on Amazon.co.jp
indexes English brand names well even with mixed JP/EN queries.

Features:
  • Full browser-like headers with ja-JP Accept-Language
  • CAPTCHA detection
  • Multiple JPY price selectors (no decimals in Japanese yen)
  • Resume, URL cache, per-pair targeting
  • Category search (&i=beauty) for tighter results
"""

import csv, json, time, random, re, argparse, logging, sys, unicodedata
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote, quote_plus, unquote

import requests  # type: ignore[import-not-found,import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-not-found]

# Optional Selenium browser mode
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

pair_seed_csv = root / "data" / "spec" / "pair_seed_catalog.csv"
legacy_pairs_csv = root / "data" / "clean" / "pink_tax_pairs.csv"
pairs_csv = pair_seed_csv if pair_seed_csv.exists() else legacy_pairs_csv
scraper_config = load_scraping_source_config(root, "amazon_japan")
output_path = cfg_path(root, scraper_config, "output_path", "data/raw/amazon_jp_raw.csv")
found_urls_path = cfg_path(
    root, scraper_config, "found_urls_path", "data/raw/amazon_jp_found_urls.json"
)

city = cfg_str(scraper_config, "city", "Tokyo")
currency = cfg_str(scraper_config, "currency", "JPY")
retailer = cfg_str(scraper_config, "retailer", "Amazon.co.jp")
today    = str(date.today())

search_delay = cfg_delay(scraper_config, "search_delay", 3.0, 7.0)
product_delay = cfg_delay(scraper_config, "product_delay", 6.0, 12.0)
session_rotate_every = cfg_int(scraper_config, "session_rotate_every", 40)
block_pause = cfg_float(scraper_config, "block_pause_seconds", 60.0)
request_timeout_seconds = cfg_float(scraper_config, "request_timeout_seconds", 15.0)
search_base_url = cfg_str(scraper_config, "search_base_url", "https://www.amazon.co.jp/s?k={query}")
product_url_template = cfg_str(
    scraper_config, "product_url_template", "https://www.amazon.co.jp/dp/{asin}"
)
referer_url = cfg_str(scraper_config, "referer_url", "https://www.amazon.co.jp")
search_index = cfg_str(scraper_config, "search_index", "beauty")
browser_wait_seconds = cfg_float(scraper_config, "browser_wait_seconds", 10.0)
allowed_brands = {
    b.strip().lower()
    for b in cfg_list(scraper_config, "allowed_brands", [])
    if b.strip()
}

default_user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
]
user_agents = cfg_list(scraper_config, "user_agents", default_user_agents)
fieldnames = [
    "pair_code", "city", "brand", "category", "gender_label", "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status",
]

# Japanese gender keywords appended to search queries
gender_kw = {"female": "女性用", "male": "男性用"}
female_kw = ["女性", "女性用", "レディース", "women", "woman", "female", "for her"]
male_kw = ["男性", "男性用", "メンズ", "men", "man", "male", "for him"]

def load_tky_products() -> list[dict]:
    """
    Load Tokyo products from pairs CSV, filtering to those with city=Tokyo.
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
            brand_query = row["brand"].split("/")[0].strip()
            if allowed_brands and brand_query.lower() not in allowed_brands:
                skipped_brand += 1
                continue
            for gender, name_col, size_col in [
                ("female", "female_product", "female_size"),
                ("male",   "male_product",   "male_size"),
            ]:
                name = row[name_col]
                size = row[size_col]
                gender_hint = gender_kw[gender]
                products.append({
                    "pair_code":    pc,
                    "gender_label": gender,
                    "product_name": name,
                    "brand":        row["brand"],
                    "category":     row["category"],
                    "size_ml_or_g": size,
                    "match_quality": row["match_quality"],
                    "search_query": build_query(name, gender, size),
                    "brand_query":  brand_query,
                    "brand_kw":     normalize_text(brand_query),
                    "gender_hint":  gender_hint,
                    "gender_kw":    female_kw if gender == "female" else male_kw,
                    "category_kw":  normalize_text(row["category"]),
                })
    if skipped_brand:
        log.info(f"Brand filter active: skipped {skipped_brand} pairs outside allowed_brands")
    log.info(f"Loaded {len(products)} products from {len(seen)} pairs ({city})")
    return products

def build_query(name: str, gender: str, size: str) -> str:
    # Use English brand + product name 
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
    kw = gender_kw[gender]
    if kw not in q:
        q += f" {kw}"
    return q

def normalize_text(text: str) -> str:
    """
    Normalize text for robust matching.
    """

    folded = unicodedata.normalize("NFKD", text)
    no_marks = "".join(ch for ch in folded if not unicodedata.combining(ch))
    low = no_marks.lower()
    return re.sub(r"[^a-z0-9\u3040-\u30ff\u4e00-\u9fff]+", " ", low).strip()


def as_text(value: Any) -> str:
    """
    Convert BeautifulSoup attribute values (str/list/None/other) to plain text.
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

def headers(referer: str = referer_url) -> dict:
    return {
        "User-Agent":                random.choice(user_agents),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             "max-age=0",
        "Referer":                   referer,
        "DNT":                       "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin",
        "Sec-Fetch-User":            "?1",
    }

block_signals = [
    "api-services-support@amazon.com",
    "/errors/validateCaptcha",
    "Robot Check",
    "ロボットによるアクセスではないことを確認",
    "不正なアクセスが検出",
]

def is_blocked(html: str) -> bool:
    low = html.lower()
    return any(s.lower() in low for s in block_signals)

def safe_get(session, url, retries=3, delay_range=search_delay):
    for attempt in range(retries):
        try:
            resp = session.get(
                url, headers=headers(), timeout=request_timeout_seconds, allow_redirects=True
            )
            if is_blocked(resp.text):
                log.warning(f"  BLOCKED ({attempt+1}/{retries}), pausing {block_pause}s")
                time.sleep(block_pause)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Request error ({attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(random.uniform(*delay_range))
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

            driver = ChromeWebDriver(
                service=ChromeService(ChromeDriverManager().install()),
                options=opts,
            )
    except Exception:
        driver = ChromeWebDriver(options=opts)
    enforce_single_window(driver)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def search_amazon_one(query: str, session, driver=None) -> BeautifulSoup | None:
    # Try beauty category first, then general
    suffixes = [f"&i={quote(search_index)}"] if search_index else [""]
    if "" not in suffixes:
        suffixes.append("")
    for suffix in suffixes:
        url = f"{search_base_url.format(query=quote(query))}{suffix}"
        if driver is not None:
            try:
                enforce_single_window(driver)
                driver.get(url)
                try:
                    WebDriverWait(driver, browser_wait_seconds).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-asin]"))
                    )
                except TimeoutException:
                    pass
                enforce_single_window(driver)
                page_html = driver.page_source
                if is_blocked(page_html):
                    continue
                return BeautifulSoup(page_html, "html.parser")
            except WebDriverException:
                continue
        else:
            resp = safe_get(session, url, delay_range=search_delay)
            if not resp or resp.status_code != 200:
                continue
            return BeautifulSoup(resp.text, "html.parser")

    return None

def pick_asin_from_soup(soup: BeautifulSoup, brand_kw: str, gender_kw: list[str]) -> str | None:
    strict: list[tuple[str, bool, str]] = []
    loose: list[tuple[str, bool, str]] = []
    norm_gender = [normalize_text(k) for k in gender_kw if normalize_text(k)]

    for el in soup.select("div[data-asin]"):
        asin = as_text(el.get("data-asin")).strip()
        if len(asin) != 10:
            continue
        title_raw = ""
        title_el = el.select_one("h2 span")
        if title_el is not None:
            title_raw = title_el.get_text(" ", strip=True)
        if not title_raw:
            title_raw = el.get_text(" ", strip=True)
        title = normalize_text(title_raw)
        if brand_kw and brand_kw not in title:
            continue
        is_ad = bool(el.find(attrs={"data-component-type": "sp-sponsored-result"}))
        has_gender = any(g in title for g in norm_gender) if norm_gender else False
        if has_gender:
            strict.append((asin, is_ad, title_raw))
        else:
            loose.append((asin, is_ad, title_raw))

    if strict:
        strict.sort(key=lambda x: (x[1],))
        asin, is_ad, title = random.choice(strict[:5])
        log.info(f"  ✓ ASIN {asin} ({'ad' if is_ad else 'organic'}) strict: {title[:60].lower()}")
        return product_url_template.format(asin=asin)
    if loose:
        loose.sort(key=lambda x: (x[1],))
        asin, is_ad, title = random.choice(loose[:5])
        log.info(f"  ✓ ASIN {asin} ({'ad' if is_ad else 'organic'}) loose-brand: {title[:60].lower()}")
        return product_url_template.format(asin=asin)
    return None

def search_amazon_jp(
    query: str,
    session,
    driver=None,
    brand_kw: str = "",
    gender_kw: list[str] | None = None,
    brand_query: str = "",
    gender_hint: str = "",
    category_kw: str = "",
) -> str | None:
    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    gkw = gender_kw or []
    for idx, current in enumerate(variants, 1):
        log.info(f"  🔍 {current} [{idx}/{len(variants)}]")
        soup = search_amazon_one(current, session, driver=driver)
        if soup is None:
            continue
        hit = pick_asin_from_soup(soup, brand_kw=brand_kw, gender_kw=gkw)
        if hit:
            return hit

    log.warning(f"  No ASIN for: {query}")
    for current in variants:
        fallback = ddg_fallback(current, session)
        if fallback:
            return fallback
    return None

def ddg_fallback(query: str, session) -> str | None:
    ddg_q = f"{query} site:amazon.co.jp/dp/"
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(ddg_q)}"
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://duckduckgo.com/",
    }
    try:
        time.sleep(random.uniform(1.0, 2.5))
        resp = session.get(ddg_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a.result__a, a[href*='amazon.co.jp']"):
            href = unquote(as_text(a.get("href")))
            m = re.search(r"https?://(?:www\.)?amazon\.co\.jp/dp/[A-Z0-9]{10}", href)
            if m:
                url = m.group(0)
                log.info(f"  ✓ DDG fallback: {url}")
                return url
    except Exception as e:
        log.warning(f"  DDG fallback error: {e}")
    return None

yen_re = re.compile(r"[¥,\s\xa0円]")

def parse_jpy(text: str) -> float | None:
    clean = yen_re.sub("", text).strip().split(".")[0]
    try:
        v = float(clean)
        return v if v >= 1 else None
    except ValueError:
        return None

def extract_price_jp(soup) -> tuple:
    price = orig = None
    promo = False

    # 1. Structured price block
    for size_attr in ("xl", "b", "l", "m"):
        blk = soup.find("span", class_="a-price", attrs={"data-a-size": size_attr})
        if blk:
            whole = blk.find("span", class_="a-price-whole")
            if whole:
                raw = re.sub(r"[^\d]", "", whole.get_text())
                try:
                    price = float(raw)
                    break
                except ValueError:
                    pass

    # 2. Legacy ID selectors
    if price is None:
        for sid in ("priceblock_ourprice", "priceblock_dealprice",
                    "priceblock_saleprice", "apex_offerDisplay_desktop"):
            el = soup.find(id=sid)
            if el:
                v = parse_jpy(el.get_text())
                if v:
                    price = v
                    break

    # 3. Offscreen price
    if price is None:
        for el in soup.find_all("span", class_="a-offscreen"):
            v = parse_jpy(el.get_text())
            if v and v >= 100:   # JPY: cheap products are still >=100
                price = v
                break

    # 4. JSON blob
    if price is None:
        for script in soup.find_all("script"):
            if script.string and "priceAmount" in script.string:
                m = re.search(r'"priceAmount"\s*:\s*([\d.]+)', script.string)
                if m:
                    try:
                        price = float(m.group(1))
                        break
                    except ValueError:
                        pass

    # Strikethrough / original
    for cls in ("a-text-strike", "a-price a-text-price"):
        el = soup.find("span", class_=cls)
        if el:
            v = parse_jpy(el.get_text())
            if v and v >= 100:
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

    cache_key = f"{product['pair_code']}|{product['gender_label']}"
    url = url_cache.get(cache_key)

    for attempt in range(2):
        if not url and not skip_search:
            url = search_amazon_jp(
                product["search_query"],
                session,
                driver=driver,
                brand_kw=product["brand_kw"],
                gender_kw=product["gender_kw"],
                brand_query=product["brand_query"],
                gender_hint=product["gender_hint"],
                category_kw=product["category_kw"],
            )
            time.sleep(random.uniform(*search_delay))

        if not url:
            row["scrape_status"] = "URL_NOT_FOUND"
            log.warning(f"  ✗ No URL: {product['product_name']}")
            return row

        row["source_url"] = url
        url_cache[cache_key] = url

        soup: BeautifulSoup | None = None
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
                    # Try plain HTTP before treating this as hard blocked.
                    resp = safe_get(session, url, delay_range=product_delay)
                    if resp and resp.status_code == 200 and not is_blocked(resp.text):
                        soup = BeautifulSoup(resp.text, "html.parser")
                    elif attempt == 0 and not skip_search:
                        url_cache.pop(cache_key, None)
                        url = ""
                        continue
                    else:
                        row["scrape_status"] = "BLOCKED"
                        return row
                else:
                    soup = BeautifulSoup(page_html, "html.parser")
            except WebDriverException:
                resp = safe_get(session, url, delay_range=product_delay)
                if resp and resp.status_code == 200 and not is_blocked(resp.text):
                    soup = BeautifulSoup(resp.text, "html.parser")
                elif attempt == 0 and not skip_search:
                    url_cache.pop(cache_key, None)
                    url = ""
                    continue
                else:
                    row["scrape_status"] = "REQUEST_ERROR"
                    return row
        else:
            resp = safe_get(session, url, delay_range=product_delay)
            if resp is None:
                if attempt == 0 and not skip_search:
                    url_cache.pop(cache_key, None)
                    url = ""
                    continue
                row["scrape_status"] = "REQUEST_ERROR"
                return row
            if resp.status_code != 200:
                if attempt == 0 and not skip_search:
                    url_cache.pop(cache_key, None)
                    url = ""
                    continue
                row["scrape_status"] = f"HTTP_{resp.status_code}"
                return row
            if is_blocked(resp.text):
                if attempt == 0 and not skip_search:
                    url_cache.pop(cache_key, None)
                    url = ""
                    continue
                row["scrape_status"] = "BLOCKED"
                return row
            soup = BeautifulSoup(resp.text, "html.parser")

        if soup is None:
            row["scrape_status"] = "REQUEST_ERROR"
            return row

        price, orig, promo = extract_price_jp(soup)
        if price is not None:
            row.update({
                "price_local":          price,
                "original_price_local": orig,
                "on_promotion":         promo,
                "confidence":           "HIGH",
                "scrape_status":        "OK",
            })
            log.info(f"  ✓ {product['gender_label']:<6} {product['product_name'][:52]:<52} ¥{price:.0f}"
                     + ("  🏷" if promo else ""))
            return row

        if attempt == 0 and not skip_search:
            url_cache.pop(cache_key, None)
            url = ""
            continue

        row["scrape_status"] = "PRICE_NOT_FOUND"
        title = soup.find("span", id="productTitle")
        log.warning(
            f"  ✗ No price: {product['product_name']}"
            + (f"  (page: {title.get_text(strip=True)[:60]})" if title else "")
        )
        return row

    return row

def main(
    dry_run=False,
    skip_search=False,
    resume=False,
    target_pair=None,
    limit=None,
    browser_mode=False,
    headful=False,
    user_data_dir="",
):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)

    products = load_tky_products()
    if target_pair:
        products = [p for p in products if p["pair_code"] == target_pair]
        if not products:
            log.error(f"Pair not found: {target_pair}")
            return

    if limit:
        keep = select_diverse_pair_codes(products, limit)
        products = [p for p in products if p["pair_code"] in keep]
        log.info(f"Limit: {len(keep)} pairs (diverse) → {len(products)} products")

    url_cache: dict = {}
    if found_urls_path.exists():
        try:
            url_cache = json.loads(found_urls_path.read_text(encoding="utf-8"))
            log.info(f"URL cache: {len(url_cache)} entries")
        except Exception:
            pass

    done_keys: set = set()
    existing: list = []
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
    results = list(existing)
    skipped = 0

    log.info(f"\n{'='*62}")
    log.info(f"  Amazon.co.jp  |  {len(products)} products  |  {'DRY RUN' if dry_run else 'LIVE'}")
    log.info(f"{'='*62}\n")

    for i, product in enumerate(products, 1):
        ck = f"{product['pair_code']}|{product['gender_label']}"
        if resume and ck in done_keys:
            skipped += 1
            continue

        log.info(f"[{i:>3}/{len(products)}] {product['pair_code']} / {product['gender_label']}")
        row = scrape_product(
            product,
            session,
            url_cache,
            dry_run=dry_run,
            skip_search=skip_search,
            driver=driver,
        )
        results.append(row)

        found_urls_path.write_text(
            json.dumps(url_cache, ensure_ascii=False, indent=2), encoding="utf-8")

        if not dry_run:
            time.sleep(random.uniform(*product_delay))
            # Rotate session every N products to reset cookies/fingerprint
            if i % session_rotate_every == 0:
                session = requests.Session()
                log.info(f"  ↺ Session rotated at product {i}")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    ok      = sum(1 for r in results if r.get("scrape_status") == "OK")
    blocked = sum(1 for r in results if r.get("scrape_status") == "BLOCKED")
    no_price= sum(1 for r in results if r.get("scrape_status") == "PRICE_NOT_FOUND")
    no_url  = sum(1 for r in results if r.get("scrape_status") == "URL_NOT_FOUND")

    log.info(f"\n{'='*62}")
    log.info(f"  OK={ok}  Blocked={blocked}  NoPrice={no_price}  NoURL={no_url}  Skipped={skipped}")
    log.info(f"  → {output_path}")
    log.info(f"  → {found_urls_path}")
    if blocked:
        log.warning(f"  {blocked} blocked. Run --resume to retry only those.")
    log.info(f"{'='*62}")
    if driver is not None:
        driver.quit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run",   action="store_true")
    ap.add_argument("--no-search", action="store_true")
    ap.add_argument("--resume",    action="store_true")
    ap.add_argument("--pair",      metavar="PAIR_CODE")
    ap.add_argument("--limit",     type=int, metavar="N",
                   help="Max number of PAIRS to scrape (products = 2×N)")
    ap.add_argument("--browser-mode", action="store_true",
                   help="Use Selenium browser mode for search + product fetch.")
    ap.add_argument("--headful", action="store_true",
                   help="Run browser mode with visible Chrome window.")
    ap.add_argument("--user-data-dir", default="",
                   help="Chrome profile directory for persistent cookies in browser mode.")
    args = ap.parse_args()
    main(dry_run=args.dry_run, skip_search=args.no_search,
         resume=args.resume, target_pair=args.pair, limit=args.limit,
         browser_mode=args.browser_mode, headful=args.headful, user_data_dir=args.user_data_dir)