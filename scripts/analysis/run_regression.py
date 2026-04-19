#!/usr/bin/env python3
"""
Statistical inference for the pink-tax paired dataset: hypothesis tests, OLS, and logit.

Outputs JSON for dashboards (data/analysis/regression_summary.json).
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = REPO_ROOT / "data/clean/pink_tax_final_dataset_cleaned.csv"
DEFAULT_OUT = REPO_ROOT / "data/analysis/regression_summary.json"

# statsmodels cluster covariance sometimes warns on F-constraints; coefficients still OK
warnings.filterwarnings(
    "ignore",
    message="covariance of constraints does not have full rank",
    category=UserWarning,
    module="statsmodels.base.model",
)


def _result_to_terms(
    res,
    max_terms: int = 120,
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    """Flatten coefficients, SEs, p-values, and 95% CIs."""
    params = res.params
    bse = res.bse
    pvals = res.pvalues
    ci = res.conf_int(alpha=0.05)
    terms: list[dict[str, Any]] = []
    for i, name in enumerate(params.index):
        if i >= max_terms:
            break
        lo, hi = float(ci.iloc[i, 0]), float(ci.iloc[i, 1])
        terms.append(
            {
                "name": str(name),
                "coef": float(params.iloc[i]),
                "std_err": float(bse.iloc[i]),
                "p_value": float(pvals.iloc[i]),
                "ci_lower": lo,
                "ci_upper": hi,
            }
        )
    fit_stats = {
        "nobs": float(res.nobs),
        "rsquared": float(res.rsquared),
        "rsquared_adj": float(res.rsquared_adj),
        "fvalue": float(res.fvalue) if res.fvalue is not None and np.isfinite(res.fvalue) else None,
        "f_pvalue": float(res.f_pvalue) if res.f_pvalue is not None and np.isfinite(res.f_pvalue) else None,
    }
    return terms, fit_stats


def _find_city_term(terms: list[dict[str, Any]]) -> dict[str, Any] | None:
    for t in terms:
        n = t["name"]
        if "Tokyo" in n and "city" in n:
            return t
    return None


def _term_by_name(terms: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for t in terms:
        if t["name"] == name:
            return t
    return None


def _logit_terms_list(res) -> list[dict[str, Any]]:
    """Flatten logit coefficients (no R² like OLS)."""
    terms: list[dict[str, Any]] = []
    bse = res.bse
    pvals = res.pvalues
    ci = res.conf_int(alpha=0.05)
    for i, name in enumerate(res.params.index):
        lo, hi = float(ci.iloc[i, 0]), float(ci.iloc[i, 1])
        terms.append(
            {
                "name": str(name),
                "coef": float(res.params.iloc[i]),
                "std_err": float(bse.iloc[i]),
                "p_value": float(pvals.iloc[i]),
                "ci_lower": lo,
                "ci_upper": hi,
            }
        )
    return terms


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add size_ratio, log price level, binary outcome for logit."""
    out = df.copy()
    male_sz = out["male_size"].replace(0, np.nan)
    out["size_ratio"] = out["female_size"] / male_sz
    mid_ppu = (out["female_ppu_local"] + out["male_ppu_local"]) / 2.0
    out["log_mean_ppu"] = np.log(np.maximum(mid_ppu, 1e-12))
    out["pink_pos"] = (out["pink_tax_pct"] > 0).astype(float)
    return out


def run_hypothesis_tests(df: pd.DataFrame) -> dict[str, Any]:
    """Paired tests on PPU; tests on pink_tax_pct; city comparison."""
    out: dict[str, Any] = {}

    fp = df["female_ppu_local"].astype(float)
    mp = df["male_ppu_local"].astype(float)
    ok = fp.notna() & mp.notna()
    fpv, mpv = fp[ok], mp[ok]
    rel = stats.ttest_rel(fpv, mpv)
    out["paired_ppu_ttest_rel"] = {
        "description": "H0: mean female PPU = mean male PPU (paired by row)",
        "n": int(fpv.shape[0]),
        "statistic": float(rel.statistic),
        "p_value": float(rel.pvalue),
    }
    try:
        wr = stats.wilcoxon(fpv, mpv, alternative="two-sided")
        out["paired_ppu_wilcoxon"] = {
            "description": "Wilcoxon signed-rank on paired female vs male PPU",
            "n": int(fpv.shape[0]),
            "statistic": float(wr.statistic) if wr.statistic is not None else None,
            "p_value": float(wr.pvalue),
        }
    except Exception as e:
        out["paired_ppu_wilcoxon"] = {"error": str(e)}

    pink = df["pink_tax_pct"].astype(float).dropna()
    o1 = stats.ttest_1samp(pink, 0.0)
    out["pink_tax_one_sample_t"] = {
        "description": "H0: mean pink_tax_pct = 0",
        "n": int(pink.shape[0]),
        "mean_pink_tax_pct": float(pink.mean()),
        "statistic": float(o1.statistic),
        "p_value": float(o1.pvalue),
    }
    try:
        wz = stats.wilcoxon(pink, alternative="two-sided")
        out["pink_tax_wilcoxon_vs_zero"] = {
            "description": "Wilcoxon signed-rank of pink_tax_pct vs 0",
            "n": int(pink.shape[0]),
            "statistic": float(wz.statistic) if wz.statistic is not None else None,
            "p_value": float(wz.pvalue),
        }
    except Exception as e:
        out["pink_tax_wilcoxon_vs_zero"] = {"error": str(e)}

    hyd = df.loc[df["city"] == "Hyderabad", "pink_tax_pct"].astype(float).dropna()
    tok = df.loc[df["city"] == "Tokyo", "pink_tax_pct"].astype(float).dropna()
    if len(hyd) >= 2 and len(tok) >= 2:
        ind = stats.ttest_ind(hyd, tok, equal_var=False)
        out["city_pink_tax_ttest_ind"] = {
            "description": "Welch t-test: pink_tax_pct Hyderabad vs Tokyo (independent)",
            "n_hyderabad": int(len(hyd)),
            "n_tokyo": int(len(tok)),
            "mean_hyderabad": float(hyd.mean()),
            "mean_tokyo": float(tok.mean()),
            "statistic": float(ind.statistic),
            "p_value": float(ind.pvalue),
        }
        try:
            mw = stats.mannwhitneyu(hyd, tok, alternative="two-sided")
            out["city_pink_tax_mann_whitney"] = {
                "description": "Mann–Whitney U on pink_tax_pct by city",
                "statistic": float(mw.statistic),
                "p_value": float(mw.pvalue),
            }
        except Exception as e:
            out["city_pink_tax_mann_whitney"] = {"error": str(e)}
    else:
        out["city_pink_tax_ttest_ind"] = {"skipped": True, "reason": "insufficient city samples"}

    return out


def _top_categories_by_mean_pink(df: pd.DataFrame, k: int = 5) -> list[dict[str, Any]]:
    g = (
        df.groupby("category", observed=True)["pink_tax_pct"]
        .agg(mean_pct="mean", n="count")
        .reset_index()
        .sort_values("mean_pct", ascending=False)
    )
    rows = []
    for _, r in g.head(k).iterrows():
        rows.append(
            {
                "category": str(r["category"]),
                "mean_pink_tax_pct": float(r["mean_pct"]),
                "n": int(r["n"]),
            }
        )
    return rows


def _direction_shares(values: pd.Series) -> dict[str, float]:
    s = values.dropna().astype(float)
    n = int(s.shape[0])
    if n == 0:
        return {"n": 0, "share_women_pay_more": None, "share_men_pay_more": None, "share_parity": None}
    pos = float((s > 0).mean())
    neg = float((s < 0).mean())
    zero = float((s == 0).mean())
    return {
        "n": n,
        "share_women_pay_more": pos,
        "share_men_pay_more": neg,
        "share_parity": zero,
    }


def _distribution_stats(values: pd.Series) -> dict[str, float | None]:
    """Mean/median/SD/IQR/range/percentiles for pink_tax_pct."""
    s = values.dropna().astype(float)
    if s.empty:
        return {k: None for k in (
            "n", "mean", "median", "std", "min", "max",
            "q1", "q3", "iqr", "p10", "p90"
        )}
    q1 = float(s.quantile(0.25))
    q3 = float(s.quantile(0.75))
    return {
        "n": int(s.shape[0]),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "std": float(s.std(ddof=1)) if s.shape[0] > 1 else None,
        "min": float(s.min()),
        "max": float(s.max()),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "p10": float(s.quantile(0.10)),
        "p90": float(s.quantile(0.90)),
    }


def _city_distribution_summary(df: pd.DataFrame) -> dict[str, Any]:
    """Per-city distribution + one-sample t-test vs 0 + direction shares.

    Reproduces the per-city numbers cited in Section 5.2 of the report.
    """
    out: dict[str, Any] = {}
    for city in sorted(df["city"].dropna().unique()):
        sub = df.loc[df["city"] == city, "pink_tax_pct"].astype(float).dropna()
        dist = _distribution_stats(sub)
        direction = _direction_shares(sub)
        ttest: dict[str, Any] = {"skipped": True}
        if sub.shape[0] >= 2:
            res = stats.ttest_1samp(sub, 0.0)
            sd = float(sub.std(ddof=1))
            se = sd / np.sqrt(sub.shape[0])
            crit = float(stats.t.ppf(0.975, df=sub.shape[0] - 1))
            mean = float(sub.mean())
            ttest = {
                "statistic": float(res.statistic),
                "p_value": float(res.pvalue),
                "ci_lower": mean - crit * se,
                "ci_upper": mean + crit * se,
            }
        out[str(city)] = {
            "distribution": dist,
            "one_sample_t_vs_zero": ttest,
            "direction": direction,
        }
    return out


def _category_table(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Reproduce Table 3: n, mean, SD, t-stat, p-value for each category.

    Sorted descending by mean pink tax %.
    """
    rows: list[dict[str, Any]] = []
    for cat, sub in df.groupby("category", observed=True)["pink_tax_pct"]:
        s = sub.dropna().astype(float)
        n = int(s.shape[0])
        if n == 0:
            continue
        mean = float(s.mean())
        sd = float(s.std(ddof=1)) if n > 1 else None
        t_stat: float | None = None
        p_val: float | None = None
        if n >= 2:
            t = stats.ttest_1samp(s, 0.0)
            t_stat = float(t.statistic)
            p_val = float(t.pvalue)
        rows.append(
            {
                "category": str(cat),
                "n": n,
                "mean_pink_tax_pct": mean,
                "std_pink_tax_pct": sd,
                "t_stat": t_stat,
                "p_value": p_val,
            }
        )
    rows.sort(key=lambda r: r["mean_pink_tax_pct"], reverse=True)
    return rows


def _city_category_diff(df: pd.DataFrame, min_n_per_city: int = 2) -> list[dict[str, Any]]:
    """Tokyo − Hyderabad mean pink tax per category (Figure 3 in report)."""
    out: list[dict[str, Any]] = []
    for cat, sub in df.groupby("category", observed=True):
        hyd = sub.loc[sub["city"] == "Hyderabad", "pink_tax_pct"].dropna().astype(float)
        tok = sub.loc[sub["city"] == "Tokyo", "pink_tax_pct"].dropna().astype(float)
        if len(hyd) < min_n_per_city or len(tok) < min_n_per_city:
            continue
        out.append(
            {
                "category": str(cat),
                "n_hyderabad": int(len(hyd)),
                "n_tokyo": int(len(tok)),
                "mean_hyderabad": float(hyd.mean()),
                "mean_tokyo": float(tok.mean()),
                "diff_tokyo_minus_hyd": float(tok.mean() - hyd.mean()),
            }
        )
    out.sort(key=lambda r: r["diff_tokyo_minus_hyd"], reverse=True)
    return out


def _retailer_summary(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Per-retailer mean / median / P90 / direction shares (Section 5.5)."""
    out: list[dict[str, Any]] = []
    for retailer, sub in df.groupby("retailer", observed=True)["pink_tax_pct"]:
        s = sub.dropna().astype(float)
        if s.empty:
            continue
        out.append(
            {
                "retailer": str(retailer),
                "n": int(s.shape[0]),
                "mean": float(s.mean()),
                "median": float(s.median()),
                "p90": float(s.quantile(0.90)),
                "p10": float(s.quantile(0.10)),
                "share_women_pay_more": float((s > 0).mean()),
                "share_men_pay_more": float((s < 0).mean()),
                "share_parity": float((s == 0).mean()),
            }
        )
    out.sort(key=lambda r: r["mean"], reverse=True)
    return out


def _brand_summary(df: pd.DataFrame, min_n: int = 5) -> list[dict[str, Any]]:
    """Per-brand mean (n>=min_n) ranked descending, like Section 5.4."""
    out: list[dict[str, Any]] = []
    for brand, sub in df.groupby("brand", observed=True)["pink_tax_pct"]:
        s = sub.dropna().astype(float)
        if s.shape[0] < min_n:
            continue
        out.append(
            {
                "brand": str(brand),
                "n": int(s.shape[0]),
                "mean": float(s.mean()),
                "median": float(s.median()),
                "std": float(s.std(ddof=1)) if s.shape[0] > 1 else None,
            }
        )
    out.sort(key=lambda r: r["mean"], reverse=True)
    return out


def _ingredient_overlap_buckets(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Reproduce Table 4: ingredient overlap buckets vs mean pink tax."""
    if "ingredient_overlap_pct" not in df.columns:
        return []
    sub = df[["ingredient_overlap_pct", "pink_tax_pct"]].dropna()
    if sub.empty:
        return []
    buckets = [
        ("0–50%", 0, 50),
        ("50–75%", 50, 75),
        ("75–90%", 75, 90),
        ("90–100%", 90, 100.0001),
    ]
    out: list[dict[str, Any]] = []
    for label, lo, hi in buckets:
        mask = (sub["ingredient_overlap_pct"] >= lo) & (sub["ingredient_overlap_pct"] < hi)
        seg = sub.loc[mask, "pink_tax_pct"].astype(float)
        if seg.empty:
            out.append({"bucket": label, "n": 0, "mean_pink_tax_pct": None,
                        "median_pink_tax_pct": None, "lo": lo, "hi": hi})
            continue
        out.append(
            {
                "bucket": label,
                "lo": float(lo),
                "hi": float(hi),
                "n": int(seg.shape[0]),
                "mean_pink_tax_pct": float(seg.mean()),
                "median_pink_tax_pct": float(seg.median()),
            }
        )
    return out


def _size_ratio_breakdown(df: pd.DataFrame, tol: float = 0.10) -> dict[str, Any]:
    """Section 4.3: % pairs at exact size ratio, within ±10%, outside."""
    if "size_ratio" not in df.columns:
        return {}
    s = df["size_ratio"].dropna().astype(float)
    if s.empty:
        return {}
    n = int(s.shape[0])
    exact = float((np.isclose(s, 1.0, atol=1e-6)).mean())
    near = float((((s - 1).abs() <= tol) & (~np.isclose(s, 1.0, atol=1e-6))).mean())
    far = float(((s - 1).abs() > tol).mean())
    return {
        "n": n,
        "share_exact_match": exact,
        "share_within_10pct": near,
        "share_outside_10pct": far,
    }


# Cleaning funnel from the published Table 2 — these counts are produced by
# scripts/cleaning/clean_pairs_dataset.py, which does not currently emit them
# as a JSON artifact; we surface them here so the website matches the report.
CLEANING_FUNNEL = [
    {"step": 1, "operation": "Deduplication by composite key",   "n_in": 325, "n_out": 318, "removed": 7},
    {"step": 2, "operation": "Drop rows missing required fields", "n_in": 318, "n_out": 312, "removed": 6},
    {"step": 3, "operation": "Drop invalid / non-positive values", "n_in": 312, "n_out": 308, "removed": 4},
    {"step": 4, "operation": "Match quality filter (MQ ≥ 3)",      "n_in": 308, "n_out": 278, "removed": 30},
]


def run_models(df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    y = "pink_tax_pct"

    # --- Model 1–5 (unchanged) ---
    f1 = f"{y} ~ C(city)"
    m1 = smf.ols(f1, data=df).fit(cov_type="HC3")
    t1, s1 = _result_to_terms(m1)
    out["m1_city_hc3"] = {"formula": f1, "cov": "HC3", "terms": t1, **s1}
    out["m1_city_hc3"]["city_tokyo"] = _find_city_term(t1)

    f2 = f"{y} ~ C(city) + C(category)"
    m2 = smf.ols(f2, data=df).fit(cov_type="HC3")
    t2, s2 = _result_to_terms(m2)
    out["m2_city_category_hc3"] = {"formula": f2, "cov": "HC3", "terms": t2, **s2}
    out["m2_city_category_hc3"]["city_tokyo"] = _find_city_term(t2)

    f3 = f"{y} ~ C(city) + C(category) + C(retailer)"
    m3 = smf.ols(f3, data=df).fit(cov_type="HC3")
    t3, s3 = _result_to_terms(m3)
    out["m3_city_category_retailer_hc3"] = {"formula": f3, "cov": "HC3", "terms": t3, **s3}
    out["m3_city_category_retailer_hc3"]["city_tokyo"] = _find_city_term(t3)

    m4 = smf.ols(f3, data=df).fit(
        cov_type="cluster",
        cov_kwds={"groups": df["brand"].astype(str)},
    )
    t4, s4 = _result_to_terms(m4)
    out["m4_city_category_retailer_cluster_brand"] = {
        "formula": f3,
        "cov": "cluster (brand)",
        "terms": t4,
        **s4,
    }
    out["m4_city_category_retailer_cluster_brand"]["city_tokyo"] = _find_city_term(t4)

    # --- Report's "M4": M3 specification + continuous ingredient_overlap_pct on n=273 ---
    sub_m4 = df.dropna(subset=["ingredient_overlap_pct"]).copy()
    f_m4 = f"{y} ~ C(city) + C(category) + C(retailer) + ingredient_overlap_pct"
    if len(sub_m4) >= 30:
        m4_paper = smf.ols(f_m4, data=sub_m4).fit(cov_type="HC3")
        t_m4, s_m4 = _result_to_terms(m4_paper)
        out["m4_paper_city_category_retailer_overlap_hc3"] = {
            "formula": f_m4,
            "cov": "HC3",
            "nobs_subset": int(len(sub_m4)),
            "terms": t_m4,
            **s_m4,
            "label": "Report Table 5 — M4 (M3 + ingredient overlap, n=273)",
        }
        out["m4_paper_city_category_retailer_overlap_hc3"]["city_tokyo"] = _find_city_term(t_m4)
        ov = _term_by_name(t_m4, "ingredient_overlap_pct")
        if ov:
            out["m4_paper_city_category_retailer_overlap_hc3"]["ingredient_overlap_pct"] = ov
    else:
        out["m4_paper_city_category_retailer_overlap_hc3"] = {
            "skipped": True,
            "reason": f"subset n={len(sub_m4)} < 30",
        }

    # Supplementary: previous m5 (city + category + overlap + jaccard, no retailer FE)
    sub = df.dropna(subset=["ingredient_overlap_pct", "jaccard_similarity"]).copy()
    f5 = f"{y} ~ C(city) + C(category) + ingredient_overlap_pct + jaccard_similarity"
    if len(sub) >= 30:
        m5 = smf.ols(f5, data=sub).fit(cov_type="HC3")
        t5, s5 = _result_to_terms(m5)
        out["m5_overlap_controls_hc3"] = {
            "formula": f5,
            "cov": "HC3",
            "nobs_subset": int(len(sub)),
            "terms": t5,
            **s5,
        }
        out["m5_overlap_controls_hc3"]["city_tokyo"] = _find_city_term(t5)
        for label in ("ingredient_overlap_pct", "jaccard_similarity"):
            hit = _term_by_name(t5, label)
            if hit:
                out["m5_overlap_controls_hc3"][label] = hit
    else:
        out["m5_overlap_controls_hc3"] = {
            "skipped": True,
            "reason": f"subset n={len(sub)} < 30",
        }

    # --- Model 6: OLS with match quality, price level, size ratio (no ingredient cols) ---
    need = [
        "pink_tax_pct",
        "city",
        "category",
        "retailer",
        "match_quality",
        "log_mean_ppu",
        "size_ratio",
    ]
    df6 = df.dropna(subset=need).copy()
    f6 = (
        f"{y} ~ C(city) + C(category) + C(retailer) + match_quality "
        "+ log_mean_ppu + size_ratio"
    )
    if len(df6) >= 40:
        m6 = smf.ols(f6, data=df6).fit(cov_type="HC3")
        t6, s6 = _result_to_terms(m6)
        out["m6_controls_price_match_size_hc3"] = {
            "formula": f6,
            "cov": "HC3",
            "terms": t6,
            **s6,
        }
        out["m6_controls_price_match_size_hc3"]["city_tokyo"] = _find_city_term(t6)
        for nm in ("match_quality", "log_mean_ppu", "size_ratio"):
            hit = _term_by_name(t6, nm)
            if hit:
                out["m6_controls_price_match_size_hc3"][nm] = hit
    else:
        out["m6_controls_price_match_size_hc3"] = {"skipped": True, "reason": f"n={len(df6)} < 40"}

    # --- Model 7: same + ingredient controls (subset with overlap) ---
    need7 = need + ["ingredient_overlap_pct", "jaccard_similarity"]
    df7 = df.dropna(subset=need7).copy()
    f7 = (
        f"{y} ~ C(city) + C(category) + C(retailer) + match_quality + log_mean_ppu "
        "+ size_ratio + ingredient_overlap_pct + jaccard_similarity"
    )
    if len(df7) >= 40:
        m7 = smf.ols(f7, data=df7).fit(cov_type="HC3")
        t7, s7 = _result_to_terms(m7)
        out["m7_full_with_ingredients_hc3"] = {
            "formula": f7,
            "cov": "HC3",
            "nobs_subset": int(len(df7)),
            "terms": t7,
            **s7,
        }
        out["m7_full_with_ingredients_hc3"]["city_tokyo"] = _find_city_term(t7)
        for nm in (
            "match_quality",
            "log_mean_ppu",
            "size_ratio",
            "ingredient_overlap_pct",
            "jaccard_similarity",
        ):
            hit = _term_by_name(t7, nm)
            if hit:
                out["m7_full_with_ingredients_hc3"][nm] = hit
    else:
        out["m7_full_with_ingredients_hc3"] = {"skipped": True, "reason": f"n={len(df7)} < 40"}

    # --- Model 8: Logit — P(pink_tax > 0) ---
    need8 = ["pink_pos", "city", "category", "retailer", "match_quality", "log_mean_ppu", "size_ratio"]
    df8 = df.dropna(subset=need8).copy()
    f8 = "pink_pos ~ C(city) + C(category) + C(retailer) + match_quality + log_mean_ppu + size_ratio"
    if len(df8) >= 60:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logit = smf.logit(f8, data=df8).fit(
                    disp=False,
                    maxiter=300,
                    method="lbfgs",
                    cov_type="HC0",
                )
            lt = _logit_terms_list(logit)
            out["m8_logit_pink_positive"] = {
                "formula": f8,
                "nobs": float(logit.nobs),
                "pseudo_rsquared": float(logit.prsquared),
                "llf": float(logit.llf),
                "terms": lt,
            }
            out["m8_logit_pink_positive"]["city_tokyo"] = _find_city_term(lt)
            conf = logit.conf_int(alpha=0.05)
            for nm in ("match_quality", "log_mean_ppu", "size_ratio"):
                hit = _term_by_name(lt, nm)
                if hit:
                    try:
                        ix = list(logit.params.index).index(nm)
                        lo = float(np.exp(conf.iloc[ix, 0]))
                        hi = float(np.exp(conf.iloc[ix, 1]))
                        extra = {
                            "odds_ratio": float(np.exp(hit["coef"])),
                            "ci_lower": lo,
                            "ci_upper": hi,
                        }
                    except Exception:
                        extra = {"odds_ratio": float(np.exp(hit["coef"]))}
                    out["m8_logit_pink_positive"][nm] = {**hit, **extra}
        except Exception as e:
            out["m8_logit_pink_positive"] = {"skipped": True, "error": str(e)}
    else:
        out["m8_logit_pink_positive"] = {"skipped": True, "reason": f"n={len(df8)} < 60"}

    return out


def _strongest_continuous_predictor(model: dict[str, Any]) -> dict[str, Any] | None:
    """Among non-intercept, non-C( terms, pick largest |z| via coef/se for key vars."""
    allow = {
        "match_quality",
        "log_mean_ppu",
        "size_ratio",
        "ingredient_overlap_pct",
        "jaccard_similarity",
    }
    best: dict[str, Any] | None = None
    best_score = 0.0
    for key in allow:
        t = model.get(key)
        if not t or t.get("std_err") in (0, None, 0.0):
            continue
        z = abs(t["coef"] / (t["std_err"] or 1e-12))
        if z > best_score:
            best_score = z
            best = {"name": key, **t, "abs_z": float(z)}
    return best


def build_conclusions(
    models: dict[str, Any],
    hypothesis: dict[str, Any],
    top_categories: list[dict[str, Any]],
    n_rows: int,
) -> dict[str, Any]:
    bullets: list[str] = []

    m1 = models.get("m1_city_hc3", {})
    m3 = models.get("m3_city_category_retailer_hc3", {})
    m4 = models.get("m4_city_category_retailer_cluster_brand", {})
    m5 = models.get("m5_overlap_controls_hc3", {})
    m6 = models.get("m6_controls_price_match_size_hc3", {})
    m7 = models.get("m7_full_with_ingredients_hc3", {})

    ct1 = m1.get("city_tokyo") or {}
    ct3 = m3.get("city_tokyo") or {}
    ct4 = m4.get("city_tokyo") or {}

    p1 = ct1.get("p_value")
    p3 = ct3.get("p_value")
    p4 = ct4.get("p_value")

    pt = hypothesis.get("paired_ppu_ttest_rel", {})
    if pt.get("p_value") is not None:
        bullets.append(
            f"Paired PPU test (same row): female vs male price-per-unit t-test p={pt['p_value']:.4g} "
            f"(n={pt.get('n', '—')}). This asks whether women’s SKUs are priced higher per unit on average."
        )

    ct = hypothesis.get("city_pink_tax_ttest_ind", {})
    if ct.get("p_value") is not None and "mean_hyderabad" in ct:
        bullets.append(
            f"City comparison (unadjusted): Hyderabad mean pink tax {ct['mean_hyderabad']:.2f}%, "
            f"Tokyo {ct['mean_tokyo']:.2f}%, Welch t-test p={ct['p_value']:.4f}."
        )

    if p1 is not None:
        bullets.append(
            f"City-only OLS (HC3): Tokyo vs Hyderabad p={p1:.4f} "
            f"(coef {ct1.get('coef', float('nan')):.2f} pp)—matches pooled city means."
        )
    if p3 is not None:
        sig = "significant at α=0.05" if p3 < 0.05 else "not significant at α=0.05"
        bullets.append(
            f"OLS with category + retailer FE (HC3): Tokyo coef {ct3.get('coef', float('nan')):.2f} pp, "
            f"p={p3:.4f} ({sig}). Interprets as conditional association, not raw city ranking."
        )
    if p4 is not None:
        bullets.append(
            f"Same FE model, SEs clustered by brand: Tokyo p={p4:.4f}."
        )

    if not m5.get("skipped"):
        io = m5.get("ingredient_overlap_pct", {})
        jc = m5.get("jaccard_similarity", {})
        if io.get("p_value") is not None:
            bullets.append(
                f"With category FE only + overlap: ingredient_overlap p={io.get('p_value', float('nan')):.4f}, "
                f"jaccard p={jc.get('p_value', float('nan')):.4f}."
            )

    if not m6.get("skipped"):
        s6 = _strongest_continuous_predictor(m6)
        if s6:
            bullets.append(
                f"Rich OLS (price level, match quality, size ratio controls): among continuous terms, "
                f"largest |t| is {s6['name']} (coef {s6['coef']:.4f}, p={s6['p_value']:.4f})."
            )

    if not m7.get("skipped"):
        io = m7.get("ingredient_overlap_pct", {})
        if io.get("p_value") is not None:
            bullets.append(
                f"Full OLS including ingredients (n={int(m7.get('nobs_subset') or m7.get('nobs') or 0)}): "
                f"ingredient_overlap_pct p={io.get('p_value', float('nan')):.4f}—"
                "even controlling for similarity, city/retailer/category structure remains in this sample."
            )

    m8 = models.get("m8_logit_pink_positive", {})
    if not m8.get("skipped") and m8.get("city_tokyo"):
        c8 = m8["city_tokyo"]
        p8 = c8.get("p_value")
        or_est = float(np.exp(c8.get("coef", 0.0)))
        if p8 is not None and np.isfinite(p8):
            bullets.append(
                f"Logit P(pink tax > 0): Tokyo vs Hyderabad OR≈{or_est:.3f}, p={p8:.4f} "
                "(marginal effect varies with X)."
            )
        else:
            bullets.append(
                f"Logit P(pink tax > 0): Tokyo coefficient (log-odds) ≈ {c8.get('coef', float('nan')):.3f} "
                f"(OR≈{or_est:.3f}); HC0 SE for the city term is not finite (quasi-separation). "
                "Use OLS on pink_tax_pct for a stable city contrast; interpret logit for the continuous controls."
            )

    summary = (
        "Hypothesis tests on paired PPUs and on pink_tax_pct, together with OLS and logit models, "
        "show where unadjusted and adjusted comparisons disagree: pooled city gaps can be small while "
        "fixed-effect models highlight channel/category confounding. Use the written conclusions for "
        "assignment-ready wording."
    )

    top_cat = top_categories[0] if top_categories else None
    written: list[str] = [
        (
            f"Dataset size: {n_rows} matched rows after cleaning. "
            "Pink tax % is defined from female vs male price per unit within each pair."
        ),
    ]
    if hypothesis.get("pink_tax_one_sample_t", {}).get("mean_pink_tax_pct") is not None:
        h0 = hypothesis["pink_tax_one_sample_t"]
        m0 = float(h0["mean_pink_tax_pct"])
        written.append(
            f"On average, the female SKU carries a {m0:.2f}% higher price per unit than the male SKU "
            f"when averaged at the pair level (mean of pink_tax_pct); the two-sided t-test against 0 "
            f"has p={h0['p_value']:.4g}."
        )
    if top_cat:
        written.append(
            f"By raw category means, the largest average pink-tax values in this sample appear in categories "
            f"such as {top_cat['category']} (mean {top_cat['mean_pink_tax_pct']:.1f}%, n={top_cat['n']}); "
            "use charts for the full distribution."
        )
    if not m3.get("skipped") and ct3.get("coef") is not None:
        written.append(
            f"In an OLS model with category and retailer fixed effects (HC3 robust SEs), "
            f"Tokyo (vs Hyderabad) is associated with about {ct3['coef']:.1f} percentage points "
            f"additional pink tax on average (p={ct3['p_value']:.4g}), holding those factors constant."
        )
    if not m6.get("skipped"):
        s6 = _strongest_continuous_predictor(m6)
        if s6:
            written.append(
                f"In the richer OLS that adds log mean PPU, size ratio, and match quality, "
                f"the strongest continuous predictor by |t| is {s6['name']} (p={s6['p_value']:.4g}). "
                "Ingredient overlap is added in the extended specification when non-missing."
            )

    return {
        "summary": summary,
        "bullets": bullets,
        "written_conclusions": written,
        "limitations": [
            "Pairs are observational; promos and pack-size quirks affect pink_tax_pct.",
            "City effects are confounded with retailer/currency context in places.",
            "Logit and OLS can diverge because they target different estimands (level vs probability).",
            "Ingredient fields are incomplete for some SKUs.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_CSV, help="Cleaned CSV path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT, help="JSON output path")
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        return 1

    df_raw = pd.read_csv(args.input)
    required = {
        "pink_tax_pct",
        "city",
        "category",
        "retailer",
        "brand",
        "female_ppu_local",
        "male_ppu_local",
        "female_size",
        "male_size",
        "match_quality",
    }
    missing = required - set(df_raw.columns)
    if missing:
        print(f"Missing columns: {sorted(missing)}", file=sys.stderr)
        return 1

    df = prepare_features(df_raw)
    hypothesis = run_hypothesis_tests(df)
    models = run_models(df)
    top_cat = _top_categories_by_mean_pink(df, k=5)

    distribution_overall = _distribution_stats(df["pink_tax_pct"])
    direction_overall = _direction_shares(df["pink_tax_pct"])
    by_city = _city_distribution_summary(df)
    category_table = _category_table(df)
    city_category_diff = _city_category_diff(df)
    retailer_summary = _retailer_summary(df)
    brand_summary = _brand_summary(df, min_n=5)
    overlap_buckets = _ingredient_overlap_buckets(df)
    size_ratio = _size_ratio_breakdown(df)

    try:
        src_rel = args.input.relative_to(REPO_ROOT)
    except ValueError:
        src_rel = args.input

    conclusions = build_conclusions(models, hypothesis, top_cat, int(len(df)))

    payload = {
        "meta": {
            "source_csv": str(src_rel),
            "n_rows": int(len(df)),
        },
        "hypothesis_tests": hypothesis,
        "descriptive": {
            "distribution_overall": distribution_overall,
            "direction_overall": direction_overall,
            "by_city": by_city,
            "category_table": category_table,
            "city_category_diff": city_category_diff,
            "retailer_summary": retailer_summary,
            "brand_summary": brand_summary,
            "ingredient_overlap_buckets": overlap_buckets,
            "size_ratio_breakdown": size_ratio,
            "top_categories_by_mean_pink_tax_pct": top_cat,
            "cleaning_funnel": CLEANING_FUNNEL,
        },
        "models": models,
        "conclusions": conclusions,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    def _sanitize(o: Any) -> Any:
        if isinstance(o, float):
            if not (np.isfinite(o)):
                return None
            return float(o)
        if isinstance(o, dict):
            return {str(k): _sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [_sanitize(v) for v in o]
        return o

    args.output.write_text(
        json.dumps(_sanitize(payload), indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
