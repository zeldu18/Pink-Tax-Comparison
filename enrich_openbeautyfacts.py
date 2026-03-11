"""
Queries the Open Beauty Facts API and appends ingredient overlap data to
the final cleaned dataset CSV.

Search strategy(3 fallback tiers per product ):
  Tier 1: brand + short product type (e.g. "Dove body wash")
  Tier 2: brand only, pick best category match
  Tier 3: curated fallback ingredient table loaded from config
"""

import csv, json, time, argparse, re, math, sys
import urllib.request, urllib.parse, urllib.error
from pathlib import Path
from collections import defaultdict

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import (
    default_obf_search_url,
    default_sleep_sec,
    default_user_agent,
    get_paths,
)

paths = get_paths(root)
pairs_csv = str(paths.pairs_csv)
cache_file = str(paths.obf_cache)
obf_search = default_obf_search_url
headers = {"User-Agent": default_user_agent}
sleep_sec = default_sleep_sec


# Load curated fallback ingredients used when OBF coverage is weak for a product.
fallback_ingredients_path = root / "config" / "cleaning" / "obf_fallback_ingredients.json"

if fallback_ingredients_path.exists():
    with fallback_ingredients_path.open(encoding="utf-8") as handle:
        fallback_ingredients: dict[str, list[str]] = json.load(handle)
else:
    fallback_ingredients = {}


def load_fallback_overrides(path: str | None) -> None:
    """
    Merge optional fallback ingredient mappings from a JSON file.
    """

    if not path:
        return
    
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    loaded = 0

    for key, ingredients in data.items():
        if isinstance(key, str) and isinstance(ingredients, list):
            fallback_ingredients[key.lower().strip()] = [
                str(i).strip().lower() for i in ingredients if str(i).strip()
            ]
            loaded += 1

def fallback_key(brand: str, category: str, gender: str) -> str:
    """
    Build lookup key for fallback_ingredients table.
    """

    brand_short = brand.split("/")[0].lower().strip()
    cat_map = {
        "Body Wash": "body wash", "Shampoo": "shampoo",
        "Conditioner": "conditioner", "Bar Soap": "bar soap",
        "Deodorant Spray": "deodorant spray", "Deodorant Roll-On": "deodorant",
        "Face Moisturizer": "face cream", "Facial Cleanser": "face wash",
        "Body Lotion": "body lotion", "Hand Cream": "hand cream",
        "Sunscreen": "sunscreen", "Toothpaste": "toothpaste",
        "Hair Oil": "hair oil", "Hair Gel / Serum": "hair serum",
        "Hair Colour": "hair colour", "Face Toner": "toner",
        "Hand Wash": "hand wash",
    }
    cat_tag = cat_map.get(category, category.lower())
    gender_tag = "women" if gender == "female" else "men"

    return f"{brand_short} {cat_tag} {gender_tag}"

def fallback_lookup(brand: str, category: str, gender: str) -> list[str] | None:
    """
    Try multiple key variants against fallback_ingredients.
    """

    brand_short = brand.split("/")[0].lower().strip()
    cat_map = {
        "Body Wash": "body wash", "Shampoo": "shampoo",
        "Conditioner": "conditioner", "Bar Soap": "bar soap",
        "Deodorant Spray": "deodorant spray", "Deodorant Roll-On": "deodorant",
        "Face Moisturizer": "moisturizer", "Facial Cleanser": "face wash",
        "Body Lotion": "body lotion", "Hand Cream": "hand cream",
        "Sunscreen": "sunscreen", "Toothpaste": "toothpaste",
        "Hair Oil": "hair oil",
    }
    cat_tag  = cat_map.get(category, "")
    g_tag    = "women" if gender == "female" else "men"
    variants = [
        f"{brand_short} {cat_tag} {g_tag}",
        f"{brand_short} {cat_tag.replace(' ', '_')} {g_tag}",
        f"{brand_short} {cat_tag}",
    ]

    for key in variants:
        if key in fallback_ingredients:
            return fallback_ingredients[key]
        
    return None

def get(url: str, retries=3) -> dict | None:

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(10)
            return None
        except Exception as e:
            if attempt < retries - 1: time.sleep(2)

    return None

category_search_terms: dict[str, tuple[str, str | None]] = {
    "Body Wash": ("body wash", "shower gel"),
    "Bar Soap": ("soap", "beauty bar"),
    "Shampoo": ("shampoo", "hair cleanser"),
    "Conditioner": ("conditioner", "hair conditioner"),
    "Hair Oil": ("hair oil", None),
    "Face Moisturizer": ("face moisturizer", "face cream"),
    "Facial Cleanser": ("face wash", "facial cleanser"),
    "Body Lotion": ("body lotion", "moisturizing lotion"),
    "Hand Cream": ("hand cream", None),
    "Deodorant Spray": ("deodorant spray", "body spray"),
    "Deodorant Roll-On": ("deodorant", "roll on"),
    "Sunscreen": ("sunscreen", "sun cream"),
    "Toothpaste": ("toothpaste", None),
    "Razor (3-blade starter kit)": ("razor", "shaving razor"),
    "Razor Cartridges": ("razor cartridges", "shaving cartridges"),
    "Hair Colour": ("hair color", "hair dye"),
    "Hair Gel / Serum": ("hair gel", "hair serum"),
    "Hand Wash": ("hand wash", "liquid soap"),
    "Face Toner": ("face toner", "skin toner"),
}

def search_obf(brand: str, product_name: str, category: str) -> dict | None:
    """
    3-tier search strategy:
      T1: brand + short category term (highest recall)
      T2: brand + first 3 words of product name
      T3: just brand name
    Returns first result with ingredient text, or None.
    """
    brand_clean = brand.split("/")[0].strip()
    name_words  = product_name.split()[:4]
    short_name  = " ".join(name_words[:3])
    cat_terms   = category_search_terms.get(category, (category.lower(), None))

    queries = []
    if cat_terms[0]:
        queries.append(f"{brand_clean} {cat_terms[0]}")
    if cat_terms[1]:
        queries.append(f"{brand_clean} {cat_terms[1]}")
    queries.append(f"{brand_clean} {short_name}")
    queries.append(brand_clean)

    for query in queries:
        params = urllib.parse.urlencode({
            "search_terms": query, "search_simple": 1, "action": "process",
            "json": 1, "page_size": 8,
            "fields": "product_name,brands,ingredients_text,ingredients_tags,categories_tags",
        })
        url  = f"{obf_search}?{params}"
        data = get(url)
        if not data:
            time.sleep(sleep_sec); continue

        products = data.get("products", [])
        with_ings = [p for p in products if p.get("ingredients_text","").strip()]

        if with_ings:
            brand_lower = brand_clean.lower()
            brand_match = [p for p in with_ings
                           if brand_lower in (p.get("brands") or "").lower()]
            return brand_match[0] if brand_match else with_ings[0]

        time.sleep(sleep_sec)

    return None

def parse_ingredients(text: str) -> list[str]:

    if not text: return []

    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"[\d.]+\s*%", "", text)
    parts = re.split(r"[,;./\n]+", text)
    out = []

    for p in parts:
        p = p.strip().lower()
        if len(p) < 3 or re.match(r"^\d+$", p): continue
        if p in {"and","or","may contain","contains","ci","color","colour"}: continue
        out.append(p)

    return out[:30]

def overlap_metrics(f_ings: list[str], m_ings: list[str]) -> dict:

    f_set, m_set = set(f_ings), set(m_ings)

    if not f_set and not m_set:
        return dict(ingredient_count_female=0, ingredient_count_male=0,
                    ingredient_overlap_pct=None, top_5_shared="",
                    female_unique_count=0, male_unique_count=0,
                    jaccard_similarity=None, data_source="none")
    
    shared = f_set & m_set
    union  = f_set | m_set
    min_n  = min(len(f_set), len(m_set)) if f_set and m_set else 1

    return dict(
        ingredient_count_female = len(f_set),
        ingredient_count_male   = len(m_set),
        ingredient_overlap_pct  = round(len(shared)/min_n*100, 1) if min_n else None,
        top_5_shared            = "|".join(sorted(shared)[:5]),
        female_unique_count     = len(f_set - m_set),
        male_unique_count       = len(m_set - f_set),
        jaccard_similarity      = round(len(shared)/len(union), 4) if union else None,
        data_source             = "api",
    )

def load_cache(path=cache_file) -> dict:
    p = Path(path)

    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def save_cache(cache: dict, path=cache_file):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

new_cols = [
    "ingredient_count_female", "ingredient_count_male",
    "ingredient_overlap_pct", "top_5_shared",
    "female_unique_count", "male_unique_count",
    "jaccard_similarity", "data_source",
    "obf_female_match", "obf_male_match",
]

def enrich(
    dry_run=False,
    target_pair=None,
    fallback_only=False,
    pairs_csv: str = pairs_csv,
    cache_file: str = cache_file,
):
    rows = []

    with open(pairs_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    for col in new_cols:
        if col not in fieldnames:
            fieldnames.append(col)
            for r in rows:
                r[col] = ""

    cache = load_cache(cache_file)
    pair_meta = {}

    for r in rows:
        pc = r["pair_code"]
        if target_pair and pc != target_pair: continue
        if pc not in pair_meta:
            pair_meta[pc] = {
                "brand": r["brand"], "category": r["category"],
                "female_product": r["female_product"],
                "male_product":   r["male_product"],
            }

    total = len(pair_meta)

    if dry_run:
        for i, (pc, meta) in enumerate(list(pair_meta.items())[:5], 1):
            print(f"  [{i}] {pc}  ->  Tier-1 query: '{meta['brand'].split('/')[0]} {category_search_terms.get(meta['category'],('?',))[0]}'")
        
        return

    enriched = {}
    api_hits = fallback_hits = no_data = 0

    for i, (pc, meta) in enumerate(pair_meta.items(), 1):
        brand    = meta["brand"]
        category = meta["category"]
        f_name   = meta["female_product"]
        m_name   = meta["male_product"]

        f_fallback = fallback_lookup(brand, category, "female")
        m_fallback = fallback_lookup(brand, category, "male")

        if f_fallback and m_fallback:
            m = overlap_metrics(f_fallback, m_fallback)
            m["data_source"] = "fallback_table"
            m["obf_female_match"] = "built-in"
            m["obf_male_match"]   = "built-in"
            enriched[pc] = m
            fallback_hits += 1
            continue

        if fallback_only:
            no_data += 1
            continue

        f_key = f"{brand}::{category}::female::{f_name[:50]}"
        m_key = f"{brand}::{category}::male::{m_name[:50]}"

        if f_key not in cache:
            print(f"    API-> female: {brand} / {category}...")
            res = search_obf(brand, f_name, category)
            cache[f_key] = res
            time.sleep(sleep_sec)
        else:
            print(f"    cache hit: female")

        if m_key not in cache:
            print(f"    API-> male:   {brand} / {category}...")
            res = search_obf(brand, m_name, category)
            cache[m_key] = res
            time.sleep(sleep_sec)
        else:
            print(f"    cache hit: male")

        save_cache(cache, cache_file)

        f_data = cache.get(f_key) or {}
        m_data = cache.get(m_key) or {}
        f_ings = parse_ingredients(f_data.get("ingredients_text", ""))
        m_ings = parse_ingredients(m_data.get("ingredients_text", ""))

        if f_ings or m_ings:
            m = overlap_metrics(f_ings, m_ings)
            m["obf_female_match"] = (f_data.get("product_name") or "")[:60]
            m["obf_male_match"]   = (m_data.get("product_name") or "")[:60]
            enriched[pc] = m
            api_hits += 1
            print(f"    ✓ API: overlap={m['ingredient_overlap_pct']}%  "
                  f"(f={m['ingredient_count_female']}, m={m['ingredient_count_male']})")
        else:
            generic_f = generic_by_category(category, "female")
            generic_m = generic_by_category(category, "male")
            if generic_f:
                m = overlap_metrics(generic_f, generic_m)
                m["data_source"] = "generic_category"
                m["obf_female_match"] = "generic"
                m["obf_male_match"]   = "generic"
                enriched[pc] = m
                fallback_hits += 1
                print(f"    ✓ Generic fallback: overlap={m['ingredient_overlap_pct']}%")
            else:
                print(f"    ✗ No data found")
                no_data += 1

    for r in rows:
        pc = r["pair_code"]
        if pc in enriched:
            for col, val in enriched[pc].items():
                r[col] = "" if val is None else val

    with open(pairs_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows)

    total_enriched = api_hits + fallback_hits
    print(f"\n{'='*55}")
    print(f"  Enrichment complete")
    print(f"  API hits:      {api_hits}")
    print(f"  Fallback hits: {fallback_hits}")
    print(f"  No data:       {no_data}")
    print(f"  Coverage:      {total_enriched}/{total} pairs "
          f"({total_enriched/total*100:.0f}%)")
    print(f"  CSV updated -> {pairs_csv}")
    print(f"  Cache saved -> {cache_file}")


def generic_by_category(category: str, gender: str) -> list[str]:
    """
    Return category-level generic ingredients when brand-specific lookup fails.
    """

    g = "women" if gender == "female" else "men"
    base = {
        "Body Wash": ["aqua","sodium laureth sulfate","cocamidopropyl betaine",
                      "glycerin","sodium chloride","parfum","citric acid","sodium benzoate"],
        "Shampoo":   ["aqua","sodium laureth sulfate","cocamidopropyl betaine",
                      "dimethicone","sodium chloride","glycerin","parfum","citric acid"],
        "Conditioner":["aqua","cetearyl alcohol","behentrimonium chloride",
                       "dimethicone","parfum","glycerin","panthenol","citric acid"],
        "Bar Soap":  ["sodium palmate","sodium palm kernelate","aqua","glycerin",
                      "stearic acid","parfum","sodium isethionate"],
        "Deodorant Spray":["butane","isobutane","propane","alcohol denat","aqua",
                           "aluminum chlorohydrate","parfum","cyclopentasiloxane"],
        "Deodorant Roll-On":["aqua","aluminum chlorohydrate","glycerin","parfum",
                             "sodium stearate","hydroxyethyl urea"],
        "Face Moisturizer":["aqua","glycerin","cetearyl alcohol","dimethicone",
                            "phenoxyethanol","parfum","panthenol"],
        "Facial Cleanser":["aqua","glycerin","sodium laureth sulfate",
                           "cocamidopropyl betaine","citric acid","parfum"],
        "Body Lotion": ["aqua","glycerin","cetearyl alcohol","dimethicone",
                        "mineral oil","parfum","phenoxyethanol","carbomer"],
        "Hand Cream":  ["aqua","glycerin","cetearyl alcohol","dimethicone",
                        "urea","parfum","phenoxyethanol"],
        "Sunscreen":   ["aqua","homosalate","ethylhexyl salicylate","avobenzone",
                        "glycerin","dimethicone","phenoxyethanol","parfum"],
        "Toothpaste":  ["water","sorbitol","hydrated silica","sodium lauryl sulfate",
                        "sodium monofluorophosphate","glycerin","sodium saccharin"],
    }
    ings = base.get(category)

    if not ings: return []
    if g == "men":
        ings = [x for x in ings if "floral" not in x]
        ings.append("menthol")
    else:
        ings.append("vitamin e")

    return ings

def main():
    global sleep_sec_over

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--fallback-only", action="store_true",
                        help="Skip API, use built-in table only (instant, offline)")
    parser.add_argument("--pair",          default=None)
    parser.add_argument("--csv",           default=pairs_csv,
                        help="Path to pairs CSV to enrich")
    parser.add_argument("--cache",         default=cache_file,
                        help="Path to OBF response cache JSON")
    parser.add_argument("--sleep",         default=sleep_sec, type=float,
                        help="Seconds to sleep between OBF API requests")
    parser.add_argument("--user-agent",    default=headers["User-Agent"],
                        help="User-Agent header for OBF API requests")
    parser.add_argument("--fallback-json", default=None,
                        help="Optional JSON file with fallback ingredient overrides")
    args = parser.parse_args()

    sleep_sec_over = args.sleep
    headers["User-Agent"] = args.user_agent
    load_fallback_overrides(args.fallback_json)

    enrich(
        dry_run=args.dry_run,
        target_pair=args.pair,
        fallback_only=args.fallback_only,
        pairs_csv=args.csv,
        cache_file=args.cache,
    )

if __name__ == "__main__":
    main()
