"""
Loads all 150 Tokyo pairs from data/clean/pink_tax_pairs.csv.

Rakuten specifics:
  • Server-rendered HTML, no JS required
  • Search: https://search.rakuten.co.jp/search/mall/{query}/
  • Product pages: item_id in URL, price in class="price"
  • Gender validation via title keywords (JP and EN)
  • DuckDuckGo site:rakuten.co.jp fallback when search returns nothing
  • Longer delays: Rakuten blocks faster than Amazon
"""

import csv, json, time, random, re, argparse, logging, sys, unicodedata
from html import unescape
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse, parse_qs

import requests  # type: ignore[import-not-found,import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-not-found]

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

pair_seed_csv   = root / "data" / "spec" / "pair_seed_catalog.csv"
legacy_pairs_csv = root / "data" / "clean" / "pink_tax_pairs.csv"
pairs_csv = pair_seed_csv if pair_seed_csv.exists() else legacy_pairs_csv
scraper_config = load_scraping_source_config(root, "rakuten_japan")
output_path = cfg_path(root, scraper_config, "output_path", "data/raw/rakuten_jp_raw.csv")
found_urls_path = cfg_path(
    root, scraper_config, "found_urls_path", "data/raw/rakuten_jp_found_urls.json"
)

city = cfg_str(scraper_config, "city", "Tokyo")
currency = cfg_str(scraper_config, "currency", "JPY")
retailer = cfg_str(scraper_config, "retailer", "Rakuten Japan")
today    = str(date.today())

search_delay = cfg_delay(scraper_config, "search_delay", 4.0, 8.0)
product_delay = cfg_delay(scraper_config, "product_delay", 6.0, 12.0)
ddg_delay = cfg_delay(scraper_config, "ddg_delay", 4.0, 8.0)
block_pause = cfg_float(scraper_config, "block_pause_seconds", 60.0)
request_timeout_seconds = cfg_float(scraper_config, "request_timeout_seconds", 18.0)
page_load_timeout_seconds = cfg_float(scraper_config, "page_load_timeout_seconds", 25.0)
search_base_url = cfg_str(
    scraper_config, "search_base_url", "https://search.rakuten.co.jp/search/mall/{query}/"
)
referer_url = cfg_str(scraper_config, "referer_url", "https://www.rakuten.co.jp")
browser_wait_seconds = cfg_float(scraper_config, "browser_wait_seconds", 10.0)
driver_get_retries = cfg_int(scraper_config, "driver_get_retries", 2)
driver_get_retry_pause = cfg_delay(scraper_config, "driver_get_retry_pause", 2.0, 4.0)
enable_ddg_fallback = cfg_str(scraper_config, "enable_ddg_fallback", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
prefer_broad_queries = cfg_str(scraper_config, "prefer_broad_queries", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
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
]
user_agents = cfg_list(scraper_config, "user_agents", default_user_agents)

# Japanese + English gender keywords for title validation
female_kw_jp = ["女性", "レディース", "女性用", "フォーハー", "ウーマン"]
female_kw_en = ["women", "woman", "female", "lady", "ladies", "venus", "her"]
female_kw = female_kw_jp + female_kw_en

male_kw_jp = ["男性", "メンズ", "男性用", "フォーヒム"]
male_kw_en = ["men", "man", "male", "mach3", "mach 3", "his", "for men"]
male_kw = male_kw_jp + male_kw_en

# Products that are gender-neutral (Curel, Nivea base line), skip gender check
gender_neutral_brands = {"curel", "biore"}

fieldnames = [
    "pair_code", "city", "brand", "category", "gender_label", "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status",
]

def load_tky_products() -> list[dict]:
    """
    Load Tokyo products from pairs CSV, build search queries.
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
            brand_lo = brand.split("/")[0].lower().strip()
            if allowed_brands and brand_lo not in allowed_brands:
                skipped_brand += 1
                continue
            neutral = brand_lo in gender_neutral_brands
            brand_query = brand.split("/")[0].strip()

            for gender, name_col, size_col in [
                ("female", "female_product", "female_size"),
                ("male",   "male_product",   "male_size"),
            ]:
                name = row[name_col]
                size = row[size_col]
                # Build JP-first search query
                gkw_jp = "女性用" if gender == "female" else "男性用"
                products.append({
                    "pair_code":    pc,
                    "gender_label": gender,
                    "product_name": name,
                    "brand":        brand,
                    "category":     row["category"],
                    "size_ml_or_g": size,
                    "match_quality": row["match_quality"],
                    "search_query": build_query(name, gkw_jp, size),
                    "gender_kw":    None if neutral else (female_kw if gender == "female" else male_kw),
                    "brand_kw":     normalize_text(brand_query),
                    "brand_query":  brand_query,
                    "gender_hint":  gkw_jp,
                    "category_kw":  normalize_text(row["category"]),
                })
    loaded_pairs = len(products) // 2
    if skipped_brand:
        log.info(f"Brand filter active: skipped {skipped_brand} pairs outside allowed_brands")
    log.info(f"Loaded {len(products)} products from {loaded_pairs} pairs ({city})")
    return products

def build_query(name: str, gkw_jp: str, size: str) -> str:
    # English product name works well on Rakuten; add JP gender keyword
    words = name.split()[:6]
    q = " ".join(words)
    try:
        sz = float(size)
        if sz > 1 and str(int(sz)) not in q:
            q += f" {int(sz)}ml" if sz < 1000 else f" {int(sz)}g"
    except (ValueError, TypeError):
        pass
    return f"{q} {gkw_jp}"

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
    broad_first = [
        f"{brand_query} {category_hint} {gender_hint}".strip(),
        f"{brand_query} {gender_hint}".strip(),
        f"{brand_query} {category_hint}".strip(),
        brand_query,
        base_query,
    ]
    specific_first = [
        base_query,
        f"{brand_query} {category_hint} {gender_hint}".strip(),
        f"{brand_query} {gender_hint}".strip(),
        f"{brand_query} {category_hint}".strip(),
        brand_query,
    ]
    variants = broad_first if prefer_broad_queries else specific_first
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
    }

hard_block_signals = [
    "不正なアクセス",
    "アクセスが遮断",
    "access denied",
    "too many requests",
    "rakuten.co.jp/error",
    "/error/r/?",
    "403 forbidden",
]

captcha_signals = [
    "captcha",
    "g-recaptcha",
    "hcaptcha",
    "cf-challenge",
    "verify you are human",
]

def is_blocked(html: str) -> bool:
    low = html.lower()
    if len(html) < 1500:
        return True
    if any(s.lower() in low for s in hard_block_signals):
        return True
    return any(s in low for s in captcha_signals)

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
            log.warning(f"  Error ({attempt+1}/{retries}): {e}")
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
    try:
        driver.set_page_load_timeout(page_load_timeout_seconds)
    except Exception:
        pass
    enforce_single_window(driver)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def safe_driver_get(driver, url: str, retries: int | None = None) -> bool:
    """
    Navigate with retries to reduce transient browser timeouts.
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

def search_rakuten(query: str, session, brand_kw: str = "",
                   gender_kw: list[str] | None = None, driver=None,
                   brand_query: str = "", gender_hint: str = "", category_kw: str = "") -> str | None:
    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    norm_gender = [normalize_text(k) for k in (gender_kw or []) if normalize_text(k)]

    # Rakuten search result cards, try multiple selector variants
    card_selectors = [
        "div.searchresultitem a.title",    # classic
        "div.item_info a",                 # alt
        "div[data-ratid] a[href*='/item']", # data-driven
        "a[href*='item.rakuten.co.jp']",   # item subdomain links
        "a[href*='.rakuten.co.jp/'][href*='/item']",
    ]

    def normalize_item_url(raw_href: str, base_url: str) -> str | None:
        href = unescape((raw_href or "").strip())
        if not href:
            return None
        if href.startswith("//"):
            href = "https:" + href
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""

        if "rakuten.co.jp" not in host:
            query_parts = parse_qs(parsed.query)
            for key in ("url", "u", "redirect", "redirect_url", "item_url"):
                values = query_parts.get(key) or []
                for value in values:
                    nested = normalize_item_url(value, base_url)
                    if nested:
                        return nested
            return None

        if host.startswith("item.rakuten.co.jp"):
            return abs_url
        return None

    def wait_for_result_links() -> None:
        if driver is None:
            return
        selectors = [
            "a[href*='item.rakuten.co.jp']",
            "div[data-ratid] a[href]",
            "div.searchresultitem a[href]",
        ]
        try:
            WebDriverWait(driver, browser_wait_seconds).until(
                lambda d: any(d.find_elements(By.CSS_SELECTOR, selector) for selector in selectors)
            )
        except TimeoutException:
            pass

    for idx, current in enumerate(variants, 1):
        search_url = search_base_url.format(query=quote(current))
        log.info(f"  🔍 {current} [{idx}/{len(variants)}]")
        if driver is not None:
            try:
                if not safe_driver_get(driver, search_url):
                    continue
                try:
                    WebDriverWait(driver, browser_wait_seconds).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                    )
                except TimeoutException:
                    pass
                wait_for_result_links()
                page_html = driver.page_source
                if is_blocked(page_html):
                    continue
                soup = BeautifulSoup(page_html, "html.parser")
            except WebDriverException:
                continue
        else:
            resp = safe_get(session, search_url, delay_range=search_delay)
            if not resp or resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

        strict: list[tuple[str, str]] = []
        loose: list[tuple[str, str]] = []
        fallback: list[tuple[str, str]] = []
        seen_urls: set[str] = set()
        for sel in card_selectors:
            cards = soup.select(sel)
            if not cards:
                continue
            for card in cards:
                href_raw = as_text(card.get("href"))
                href = normalize_item_url(href_raw, search_url)
                if not href or href in seen_urls:
                    continue
                seen_urls.add(href)
                title_raw = (
                    card.get_text(" ", strip=True)
                    or as_text(card.get("title"))
                    or as_text(card.get("aria-label"))
                )
                title = normalize_text(title_raw)
                has_brand = not brand_kw or brand_kw in title or brand_kw in normalize_text(href)
                has_gender = any(g in title for g in norm_gender) if norm_gender else False
                if has_brand and has_gender:
                    strict.append((href, title_raw))
                elif has_brand or has_gender:
                    loose.append((href, title_raw))
                else:
                    fallback.append((href, title_raw))
            if strict or loose:
                break

        if strict:
            href, title = random.choice(strict[:5])
            log.info(f"  ✓ Rakuten native strict: {title[:60].lower()}")
            return href
        if loose:
            href, title = random.choice(loose[:5])
            log.info(f"  ✓ Rakuten native loose-brand: {title[:60].lower()}")
            return href
        if fallback:
            href, title = random.choice(fallback[:5])
            log.info(f"  ✓ Rakuten native unfiltered: {title[:60].lower()}")
            return href

    if enable_ddg_fallback:
        log.warning("  No Rakuten native result, trying DDG")
        for current in variants:
            ddg_match = ddg_fallback(current, session)
            if ddg_match:
                return ddg_match
    else:
        log.warning("  No Rakuten native result, DDG fallback disabled")
    return None

def ddg_fallback(query: str, session) -> str | None:
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote(query + ' site:item.rakuten.co.jp')}"
    headers = {
        "User-Agent":      random.choice(user_agents),
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Accept":          "text/html,application/xhtml+xml",
        "Referer":         "https://duckduckgo.com/",
    }
    try:
        time.sleep(random.uniform(*ddg_delay))
        resp = session.get(ddg_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a.result__a, a[href*='rakuten.co.jp']"):
            href = as_text(a.get("href")).strip()
            if "rakuten.co.jp" in href and ("/item" in href or "item_" in href):
                if not href.startswith("http"):
                    href = "https://" + href.lstrip("/")
                log.info(f"  ✓ DDG fallback: {href[:70]}")
                return href
    except Exception as e:
        log.warning(f"  DDG fallback error: {e}")
    return None

yen_re = re.compile(r"[¥,\s\xa0円]")

def parse_jpy(text: str) -> float | None:
    clean = yen_re.sub("", text).strip().split(".")[0]
    try:
        v = float(clean)
        return v if v >= 100 else None
    except ValueError:
        return None

def extract_price_rakuten(soup) -> tuple:
    price = orig = None
    promo = False

    meta_item_price = soup.select_one("meta[itemprop='price'][content]")
    if meta_item_price:
        v = parse_jpy(meta_item_price.get("content", ""))
        if v:
            price = v

    if price is None:
        el = soup.select_one("#itemPrice")
        if el:
            v = parse_jpy(el.get_text(" ", strip=True))
            if v:
                price = v

    if price is None:
        for selector in (
            "meta[property='product:price:amount'][content]",
            "div[class*='item-price']",
            "[class*='price']",
            "[id*='price']",
        ):
            el = soup.select_one(selector)
            if not el:
                continue
            v = parse_jpy(el.get("content", "") if el.has_attr("content") else el.get_text(" ", strip=True))
            if v:
                price = v
                break

    # 2. JSON-LD
    if price is None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                offers = data.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                p = offers.get("price") or offers.get("lowPrice")
                if p:
                    price = float(p)
                    break
            except Exception:
                continue

    # 3. Regex fallback
    if price is None:
        whole_html = str(soup)
        m_item = re.search(r'id=["\']itemPrice["\'][^>]*>.*?([\d,]{3,})\s*円', whole_html, flags=re.DOTALL)
        if m_item:
            v = parse_jpy(m_item.group(1))
            if v:
                price = v
        if price is None:
            candidates = [parse_jpy(m.group(0)) for m in re.finditer(r"[\d,]{3,}\s*円|[¥￥]\s*[\d,]{3,}", soup.get_text(" ", strip=True))]
            candidates = [c for c in candidates if c]
            if candidates:
                price = max(candidates)

    # Original / sale price
    for cls in ("price2", "priceold", "price_before", "_2eULlq"):
        el = soup.find(class_=cls)
        if el:
            v = parse_jpy(el.get_text())
            if v:
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
    url = url_cache.get(ck)

    if not url and not skip_search:
        url = search_rakuten(product["search_query"], session,
                             brand_kw=product["brand_kw"],
                             gender_kw=product["gender_kw"],
                             driver=driver,
                             brand_query=product["brand_query"],
                             gender_hint=product["gender_hint"],
                             category_kw=product["category_kw"])
        time.sleep(random.uniform(*search_delay))

    if not url:
        row["scrape_status"] = "URL_NOT_FOUND"
        log.warning(f"  ✗ No URL: {product['product_name']}")
        return row

    row["source_url"] = url
    url_cache[ck]     = url

    if driver is not None:
        try:
            if not safe_driver_get(driver, url):
                row["scrape_status"] = "REQUEST_ERROR"
                return row
            try:
                WebDriverWait(driver, browser_wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                )
            except TimeoutException:
                pass
            soup = BeautifulSoup(driver.page_source, "html.parser")
        except WebDriverException:
            row["scrape_status"] = "REQUEST_ERROR"
            return row
    else:
        resp = safe_get(session, url, delay_range=product_delay)
        if resp is None:
            row["scrape_status"] = "REQUEST_ERROR"
            return row
        if resp.status_code != 200:
            row["scrape_status"] = f"HTTP_{resp.status_code}"
            return row
        soup = BeautifulSoup(resp.text, "html.parser")
    price, orig, promo = extract_price_rakuten(soup)

    if price is None:
        row["scrape_status"] = "PRICE_NOT_FOUND"
        log.warning(f"  ✗ No price: {product['product_name']}")
    else:
        row.update({
            "price_local": price, "original_price_local": orig,
            "on_promotion": promo, "confidence": "HIGH", "scrape_status": "OK",
        })
        log.info(f"  ✓ {product['gender_label']:<6} {product['product_name'][:52]:<52} ¥{price:.0f}"
                 + ("  🏷" if promo else ""))

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
            log.error(f"Pair not found: {target_pair}"); return

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
    log.info(f"  Rakuten Japan  |  {len(products)} products  |  {'DRY RUN' if dry_run else 'LIVE'}")
    log.info(f"  DDG fallback   |  {'ON' if enable_ddg_fallback else 'OFF'}")
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
        log.warning(f"  ⚠ {blocked} blocked. Run --resume to retry.")
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
