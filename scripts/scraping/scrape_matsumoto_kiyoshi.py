"""
Loads all 150 Tokyo pairs from data/clean/pink_tax_pairs.csv.
Matsumoto Kiyoshi is Japan's largest pharmacy chain; server-rendered,
easy to parse, no JS required.

Search: https://www.matsukiyococokara-online.com/store/catalogsearch/result/?q={query}
Product pages: https://www.matsukiyococokara-online.com/store/catalog/product/view/id/{product_id}

Features:
  • CSV-driven, no hardcoded product list
  • Native search + DuckDuckGo fallback
  • Resume + URL cache
  • Gender validation via JP + EN keywords
"""

import csv, json, time, random, re, argparse, logging, sys, unicodedata
from datetime import date
from pathlib import Path
from urllib.parse import quote
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
    cfg_list,
    cfg_path,
    cfg_str,
    load_scraping_source_config,
)
from pink_tax.utils import select_diverse_pair_codes

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

pair_seed_csv = root / "data" / "spec" / "pair_seed_catalog.csv"
legacy_pairs_csv = root / "data" / "clean" / "pink_tax_pairs.csv"
pairs_csv = pair_seed_csv if pair_seed_csv.exists() else legacy_pairs_csv
scraper_config = load_scraping_source_config(root, "matsumoto_kiyoshi")
output_path = cfg_path(root, scraper_config, "output_path", "data/raw/matsumoto_raw.csv")
found_urls_path = cfg_path(
    root, scraper_config, "found_urls_path", "data/raw/matsumoto_found_urls.json"
)

city = cfg_str(scraper_config, "city", "Tokyo")
currency = cfg_str(scraper_config, "currency", "JPY")
retailer = cfg_str(scraper_config, "retailer", "Matsumoto Kiyoshi")
today = str(date.today())

search_delay = cfg_delay(scraper_config, "search_delay", 3.0, 6.0)
product_delay = cfg_delay(scraper_config, "product_delay", 4.0, 8.0)
ddg_delay = cfg_delay(scraper_config, "ddg_delay", 4.0, 7.0)
request_timeout_seconds = cfg_float(scraper_config, "request_timeout_seconds", 20.0)
page_load_timeout_seconds = cfg_float(scraper_config, "page_load_timeout_seconds", 25.0)
search_base_url = cfg_str(
    scraper_config, "search_base_url", "https://www.matsukiyococokara-online.com/store/catalogsearch/result/?q={query}"
)
referer_url = cfg_str(scraper_config, "referer_url", "https://www.matsukiyococokara-online.com/store/")
site_base_url = cfg_str(scraper_config, "site_base_url", "https://www.matsukiyococokara-online.com")
browser_wait_seconds = cfg_float(scraper_config, "browser_wait_seconds", 10.0)
enable_ddg_fallback = cfg_str(scraper_config, "enable_ddg_fallback", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
prefer_broad_queries = cfg_str(scraper_config, "prefer_broad_queries", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
allow_unfiltered_fallback = cfg_str(scraper_config, "allow_unfiltered_fallback", "false").strip().lower() in {
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
]
user_agents = cfg_list(scraper_config, "user_agents", default_user_agents)

female_kw = ["女性", "レディース", "女性用", "women", "woman", "female", "lady", "venus"]
male_kw = ["男性", "メンズ", "男性用", "men", "man", "male", "mach3"]

brand_aliases: dict[str, list[str]] = {
    "dove": ["ダヴ"],
    "bioré": ["ビオレ", "biore"],
    "biore": ["ビオレ", "bioré"],
    "nivea": ["ニベア"],
    "kao": ["花王", "カオー"],
    "curel": ["キュレル"],
    "kose": ["コーセー"],
    "softymo": ["ソフティモ"],
    "mandom": ["マンダム", "gatsby", "ギャツビー"],
    "dhc": ["ディーエイチシー"],
    "pola": ["ポーラ"],
    "shiseido": ["資生堂"],
    "gillette": ["ジレット", "venus", "ヴィーナス"],
    "lion": ["ライオン"],
}

fieldnames = [
    "pair_code", "city", "brand", "category", "gender_label", "product_name",
    "size_ml_or_g", "price_local", "currency", "original_price_local",
    "on_promotion", "retailer", "match_quality", "confidence",
    "date_scraped", "source_url", "scrape_status",
]

def load_tky_products() -> list[dict]:
    """
    Load products from the Tokyo pairs CSV.
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
            brand    = row["brand"]
            brand_query = brand.split("/")[0].strip()
            brand_lo = brand_query.lower().strip()
            if allowed_brands and brand_lo not in allowed_brands:
                skipped_brand += 1
                continue
            aliases = brand_aliases.get(brand_lo, [])
            brand_terms = [normalize_text(brand_query)] + [
                normalize_text(alias) for alias in aliases
            ]
            seen_terms: set[str] = set()
            brand_terms = [term for term in brand_terms if term and not (term in seen_terms or seen_terms.add(term))]

            for gender, name_col, size_col in [
                ("female", "female_product", "female_size"),
                ("male",   "male_product",   "male_size"),
            ]:
                name = row[name_col]
                size = row[size_col]
                gkw  = "女性用" if gender == "female" else "男性用"
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
                    "brand_terms":  brand_terms,
                    "brand_query":  brand_query,
                    "gender_hint":  gkw,
                    "category_kw":  normalize_text(row["category"]),
                })
    if skipped_brand:
        log.info(f"Brand filter active: skipped {skipped_brand} pairs outside allowed_brands")
    log.info(f"Loaded {len(products)} products from {len(products) // 2} pairs ({city})")
    return products

def build_query(name: str, gkw: str, size: str) -> str:
    words = name.split()[:6]
    q = " ".join(words)
    try:
        sz = float(size)
        if sz > 1 and str(int(sz)) not in q:
            q += f" {int(sz)}ml" if sz < 1000 else f" {int(sz)}g"
    except (ValueError, TypeError):
        pass
    return f"{q} {gkw}"

def normalize_text(text: str) -> str:
    """
    Normalize text for robust matching.
    """

    folded = unicodedata.normalize("NFKD", text)
    no_marks = "".join(ch for ch in folded if not unicodedata.combining(ch))
    low = no_marks.lower()
    return re.sub(r"[^a-z0-9\u3040-\u30ff\u4e00-\u9fff]+", " ", low).strip()

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
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "ja-JP,ja;q=0.9,en;q=0.8",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             "max-age=0",
        "Referer":                   referer,
        "DNT":                       "1",
    }

def safe_get(session, url, retries=3, delay_range=search_delay):
    for attempt in range(retries):
        try:
            resp = session.get(
                url, headers=headers(), timeout=request_timeout_seconds, allow_redirects=True
            )
            if resp.status_code == 200:
                return resp
            log.warning(f"  HTTP {resp.status_code} ({attempt+1}/{retries})")
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout ({attempt+1}/{retries}): {url[:70]}")
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
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument(f"--user-agent={random.choice(user_agents)}")
    driver = None
    try:
        from selenium.webdriver.chrome.service import Service  # type: ignore[import-not-found]

        cached = cached_chromedriver_path()
        if cached:
            driver = webdriver.Chrome(service=Service(cached), options=opts)
        else:
            from webdriver_manager.chrome import ChromeDriverManager  # type: ignore[import-not-found]

            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=opts,
            )
    except Exception:
        driver = webdriver.Chrome(options=opts)
    try:
        driver.set_page_load_timeout(page_load_timeout_seconds)
    except Exception:
        pass
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def search_matsumoto(query: str, session,
                     brand_kw: str = "", brand_terms: list[str] | None = None, gender_kw: list[str] | None = None,
                     driver=None,
                     brand_query: str = "", gender_hint: str = "", category_kw: str = "") -> str | None:
    variants = build_query_variants(query, brand_query or brand_kw, category_kw, gender_hint)
    norm_gender = [normalize_text(k) for k in (gender_kw or []) if normalize_text(k)]

    # Product card selectors for Matsukiyo Cocokara store
    selectors = [
        "a[href*='/store/catalog/product/view/id/']",
        "div.product-item-info a",
        "ol.products li a",
        "ul.products li a",
        "div.item-info a",
        "div.productNameArea a",
    ]
    for idx, current in enumerate(variants, 1):
        search_url = search_base_url.format(query=quote(current))
        log.info(f"  🔍 {current} [{idx}/{len(variants)}]")
        if driver is not None:
            try:
                driver.get(search_url)
                try:
                    WebDriverWait(driver, browser_wait_seconds).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
                    )
                except TimeoutException:
                    pass
                soup = BeautifulSoup(driver.page_source, "html.parser")
            except WebDriverException:
                continue
        else:
            resp = safe_get(session, search_url, delay_range=search_delay)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

        strict: list[tuple[str, str]] = []
        loose: list[tuple[str, str]] = []
        fallback: list[tuple[str, str]] = []
        terms = [t for t in (brand_terms or []) if t]
        if brand_kw and brand_kw not in terms:
            terms.append(brand_kw)
        for sel in selectors:
            for card in soup.select(sel):
                href = card.get("href", "")
                title_raw = (
                    card.get_text(" ", strip=True)
                    or card.get("title", "")
                    or card.get("aria-label", "")
                )
                title = normalize_text(title_raw)
                if not href:
                    continue
                href_norm = href.lower()
                if (
                    "/store/catalog/product/view/id/" not in href_norm
                    and "matsukiyococokara-online.com/store/catalog/product/view/id/" not in href_norm
                ):
                    continue
                has_brand = not terms or any(
                    term in title or term in normalize_text(href)
                    for term in terms
                )
                if not href.startswith("http"):
                    href = f"{site_base_url}{href}"
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
            log.info(f"  ✓ MK native strict: {title[:60].lower()}")
            return href
        if loose:
            href, title = random.choice(loose[:5])
            log.info(f"  ✓ MK native loose-brand: {title[:60].lower()}")
            return href
        if allow_unfiltered_fallback and fallback:
            href, title = random.choice(fallback[:5])
            log.info(f"  ✓ MK native unfiltered: {title[:60].lower()}")
            return href

    if enable_ddg_fallback:
        log.warning("  No MK native result, trying DDG")
        for current in variants:
            ddg_match = ddg_fallback(current, session)
            if ddg_match:
                return ddg_match
    else:
        log.warning("  No MK native result, DDG fallback disabled")
    return None

def ddg_fallback(query: str, session) -> str | None:
    ddg_url = (f"https://html.duckduckgo.com/html/?q="
               f"{quote(query + ' site:matsukiyococokara-online.com')}")
    headers = {
        "User-Agent":      random.choice(user_agents),
        "Accept-Language": "ja-JP,ja;q=0.9",
        "Accept":          "text/html,application/xhtml+xml",
        "Referer":         "https://duckduckgo.com/",
    }
    try:
        time.sleep(random.uniform(*ddg_delay))
        resp = session.get(ddg_url, headers=headers, timeout=request_timeout_seconds)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a.result__a, a[href]"):
            href = a.get("href", "")
            if "matsukiyococokara-online.com/store/catalog/product/view/id/" in href:
                if not href.startswith("http"):
                    href = "https://" + href.lstrip("/")
                log.info(f"  ✓ DDG fallback: {href[:70]}")
                return href
    except Exception as e:
        log.warning(f"  DDG fallback error: {e}")
    return None

yen_re = re.compile(r"[¥,\s\xa0円（）税込]")

def parse_jpy(text: str) -> float | None:
    clean = yen_re.sub("", text).strip().split(".")[0]
    m = re.search(r"(\d+)", clean)
    if m:
        try:
            v = float(m.group(1))
            return v if v >= 100 else None
        except ValueError:
            pass
    return None

def extract_price_matsumoto(soup) -> tuple:
    price = orig = None
    promo = False

    # 1. Structured selectors
    for tag, attrs in [
        ("span", {"class": "price"}),
        ("p",    {"class": "price"}),
        ("span", {"itemprop": "price"}),
        ("div",  {"class": "price-box"}),
        ("span", {"class": "selling-price"}),
        ("strong", {"class": "price"}),
        ("div",  {"class": "priceArea"}),
    ]:
        el = soup.find(tag, attrs)
        if el:
            v = parse_jpy(el.get_text())
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
                p = offers.get("price")
                if p:
                    price = float(p)
                    break
            except Exception:
                continue

    # 3. Text fallback for current storefront templates
    if price is None:
        text = soup.get_text(" ", strip=True)
        patterns = [
            r"税率\d+％\s*([\d,]+)円（税込）",
            r"税込\s*([\d,]+)円",
            r"本体\s*([\d,]+)円",
            r"([\d,]+)円（税込）",
            r"[¥￥]\s*([\d,]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if not m:
                continue
            v = parse_jpy(m.group(1) if m.groups() else m.group(0))
            if v:
                price = v
                break

    # Strikethrough / original
    for tag in ["del", "s"]:
        el = soup.find(tag)
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
        url = search_matsumoto(product["search_query"], session,
                               brand_kw=product["brand_kw"],
                               brand_terms=product.get("brand_terms"),
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
            driver.get(url)
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
    price, orig, promo = extract_price_matsumoto(soup)

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
    if browser_mode:
        if not selenium_ok:
            log.error("Selenium not installed. Run: pip install selenium webdriver-manager")
            return
        driver = build_driver(headless=not headful, user_data_dir=user_data_dir)
    results, skipped = list(existing), 0

    log.info(f"\n{'='*62}")
    log.info(f"  Matsumoto Kiyoshi  |  {len(products)} products  |  {'DRY RUN' if dry_run else 'LIVE'}")
    log.info(f"  DDG fallback       |  {'ON' if enable_ddg_fallback else 'OFF'}")
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
    blocked = sum(1 for r in results if r.get("scrape_status") in ("BLOCKED", "REQUEST_ERROR"))
    no_url  = sum(1 for r in results if r.get("scrape_status") == "URL_NOT_FOUND")
    no_price= sum(1 for r in results if r.get("scrape_status") == "PRICE_NOT_FOUND")

    log.info(f"\n{'='*62}")
    log.info(f"  OK={ok}  Errors={blocked}  NoPrice={no_price}  NoURL={no_url}  Skipped={skipped}")
    log.info(f"  → {output_path}")
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
