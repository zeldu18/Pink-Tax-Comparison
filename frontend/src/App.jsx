import { useEffect, useMemo, useRef, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  ComposedChart,
  Bar,
  Cell,
  Line,
  LabelList,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ScatterChart,
  Scatter,
  ReferenceLine
} from "recharts";

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function avg(values) {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function quantile(values, q) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (sorted[base + 1] !== undefined) {
    return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
  }
  return sorted[base];
}

function stdDev(values) {
  if (!values.length) return 0;
  const m = avg(values);
  return Math.sqrt(avg(values.map((v) => (v - m) ** 2)));
}

function trimmedMean(values, trimShare = 0.1) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const cut = Math.floor(sorted.length * trimShare);
  const start = clamp(cut, 0, Math.max(0, sorted.length - 1));
  const end = clamp(sorted.length - cut, 1, sorted.length);
  const core = sorted.slice(start, end);
  return core.length ? avg(core) : avg(sorted);
}

function buildGroupStats(rows, field) {
  const map = new Map();
  for (const row of rows) {
    const key = row[field];
    const pink = toNum(row.pink_tax_pct);
    if (!key || pink === null) continue;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(pink);
  }
  return [...map.entries()].map(([key, vals]) => {
    const pos = vals.filter((v) => v > 0).length;
    const neg = vals.filter((v) => v < 0).length;
    const zero = vals.filter((v) => v === 0).length;
    return {
      key,
      n: vals.length,
      mean: Number(avg(vals).toFixed(2)),
      median: Number(median(vals).toFixed(2)),
      q1: Number(quantile(vals, 0.25).toFixed(2)),
      q3: Number(quantile(vals, 0.75).toFixed(2)),
      p10: Number(quantile(vals, 0.10).toFixed(2)),
      p90: Number(quantile(vals, 0.90).toFixed(2)),
      positiveShare: Number(((pos / vals.length) * 100).toFixed(1)),
      negativeShare: Number(((neg / vals.length) * 100).toFixed(1)),
      zeroShare: Number(((zero / vals.length) * 100).toFixed(1))
    };
  });
}

function histogram(rows, bins = 28) {
  const vals = rows.map((r) => toNum(r.pink_tax_pct)).filter((v) => v !== null);
  if (!vals.length) return [];
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const width = (max - min || 1) / bins;
  const out = Array.from({ length: bins }, (_, i) => {
    const left = min + i * width;
    const right = min + (i + 1) * width;
    return {
      bucketLabel: `${left.toFixed(1)} to ${right.toFixed(1)}`,
      bucketMid: Number(((left + right) / 2).toFixed(2)),
      count: 0
    };
  });
  for (const v of vals) {
    const idx = Math.min(bins - 1, Math.max(0, Math.floor((v - min) / width)));
    out[idx].count += 1;
  }
  return out;
}

function statCard(label, value, help = "") {
  const valueText = String(value ?? "");
  return (
    <article className="card" key={label}>
      <div className="label">{label}</div>
      <div className={`value ${valueText.length > 22 ? "value-long" : ""}`}>{value}</div>
      {help ? <div className="help">{help}</div> : null}
    </article>
  );
}

function pct(n) {
  return `${Number(n).toFixed(1)}%`;
}

function shortLabel(value, max = 18) {
  const text = String(value ?? "");
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function axisLabelX(value) {
  return { value, position: "insideBottom", offset: -8 };
}

function axisLabelY(value) {
  return { value, angle: -90, position: "insideLeft" };
}

function axisLabelXCentered(value) {
  return { value, position: "insideBottom", offset: -4, style: { textAnchor: "middle" } };
}

function axisLabelYCentered(value) {
  return { value, angle: -90, position: "insideLeft", style: { textAnchor: "middle" } };
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

const CATEGORY_BAR_COLORS = ["#9d174d", "#be185d", "#db2777", "#ec4899", "#f9a8d4"];

function fmtPct(v, digits = 2) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "—";
  return `${Number(v).toFixed(digits)}%`;
}
function fmtNum(v, digits = 2) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "—";
  return Number(v).toFixed(digits);
}
function fmtP(p) {
  if (p === null || p === undefined || !Number.isFinite(Number(p))) return "—";
  const n = Number(p);
  if (n < 0.001) return "<0.001";
  return n.toFixed(3);
}
function pStars(p) {
  if (p === null || p === undefined || !Number.isFinite(Number(p))) return "";
  const n = Number(p);
  if (n < 0.001) return "***";
  if (n < 0.05) return "**";
  if (n < 0.1) return "*";
  return "";
}
function fmtPpt(v, digits = 2) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "—";
  const n = Number(v);
  return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}`;
}

function StatsHeroCard({ value, label, subtext, variant = "neutral" }) {
  return (
    <article className={`stats-hero-card stats-hero-card--${variant}`}>
      <div className="stats-hero-value">{value}</div>
      <div className="stats-hero-label">{label}</div>
      {subtext ? <div className="stats-hero-subtext">{subtext}</div> : null}
    </article>
  );
}

function CategoryBarChart({ data, valueKey = "mean", labelKey = "category", colors = CATEGORY_BAR_COLORS, suffix = "%" }) {
  if (!data?.length) return null;
  const vals = data.map((d) => Number(d[valueKey]) || 0);
  const minV = Math.min(0, ...vals);
  const maxV = Math.max(0, ...vals);
  const pad = Math.max(8, (maxV - minV) * 0.08);
  const xMin = Math.floor((minV - pad) / 10) * 10;
  const xMax = Math.ceil((maxV + pad) / 10) * 10;
  const height = Math.max(260, data.length * 32 + 60);
  return (
    <div className="stats-chart">
      <ResponsiveContainer width="100%" height={height}>
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 12, right: 72, left: 16, bottom: 12 }}
          barCategoryGap={6}
        >
          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
          <XAxis
            type="number"
            domain={[xMin, xMax]}
            tick={{ fontSize: 12, fill: "#475569" }}
            tickFormatter={(v) => `${v}${suffix}`}
          />
          <YAxis
            type="category"
            dataKey={labelKey}
            width={150}
            tick={{ fontSize: 12, fill: "#0f172a", fontWeight: 500 }}
            tickLine={false}
            axisLine={false}
          />
          <Tooltip
            cursor={{ fill: "rgba(236, 72, 153, 0.08)" }}
            formatter={(value, _name, item) => [
              `${Number(value).toFixed(1)}${suffix}`,
              `n = ${item?.payload?.n ?? "—"}`
            ]}
          />
          {minV < 0 && <ReferenceLine x={0} stroke="#94a3b8" />}
          <Bar dataKey={valueKey} radius={[0, 6, 6, 0]} isAnimationActive={false}>
            {data.map((entry, i) => {
              const v = Number(entry[valueKey]);
              const fill = v < 0
                ? "#1d4ed8"
                : colors[i % colors.length];
              return <Cell key={`${entry[labelKey]}-${i}`} fill={fill} />;
            })}
            <LabelList
              dataKey={valueKey}
              position="right"
              formatter={(v) => `${Number(v).toFixed(1)}${suffix}`}
              style={{ fill: "#0f172a", fontSize: 11, fontWeight: 600 }}
            />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function StatisticsPanel({ data }) {
  const desc = data?.descriptive ?? {};
  const models = data?.models ?? {};
  const hyp = data?.hypothesis_tests ?? {};

  const dist = desc.distribution_overall ?? {};
  const direction = desc.direction_overall ?? {};
  const byCity = desc.by_city ?? {};
  const hyd = byCity.Hyderabad ?? {};
  const tok = byCity.Tokyo ?? {};
  const catTable = desc.category_table ?? [];
  const cityCatDiff = desc.city_category_diff ?? [];
  const retailerSummary = desc.retailer_summary ?? [];
  const brandSummary = desc.brand_summary ?? [];
  const overlapBuckets = desc.ingredient_overlap_buckets ?? [];
  const sizeRatio = desc.size_ratio_breakdown ?? {};
  const cleaningFunnel = desc.cleaning_funnel ?? [];

  const m1 = models.m1_city_hc3;
  const m2 = models.m2_city_category_hc3;
  const m3 = models.m3_city_category_retailer_hc3;
  const m4Paper = models.m4_paper_city_category_retailer_overlap_hc3;
  const m6 = models.m6_controls_price_match_size_hc3;

  const meanOverall = dist.mean;
  const oneSampleP = hyp.pink_tax_one_sample_t?.p_value;

  const topCat = catTable[0];
  const topCatLabel = topCat ? `${topCat.category}` : "—";
  const topCatSubtext = topCat ? `${fmtPct(topCat.mean_pink_tax_pct, 1)} mean (n=${topCat.n})` : "";

  const cityCardScale = Math.max(
    Math.abs(Number(hyd.distribution?.mean) || 0),
    Math.abs(Number(tok.distribution?.mean) || 0),
    1
  );

  const top5Cats = catTable.slice(0, 5).map((c) => ({
    category: c.category,
    mean: Number(c.mean_pink_tax_pct),
    n: c.n,
  }));

  const cityDiffTop = [...cityCatDiff]
    .sort((a, b) => b.diff_tokyo_minus_hyd - a.diff_tokyo_minus_hyd)
    .map((d) => ({
      category: d.category,
      mean: Number(d.diff_tokyo_minus_hyd),
      n: `${d.n_hyderabad} vs ${d.n_tokyo}`,
    }));

  const overlapBars = overlapBuckets.map((b) => ({
    category: b.bucket,
    mean: Number(b.mean_pink_tax_pct ?? 0),
    n: b.n,
  }));

  const cityP = hyp.city_pink_tax_ttest_ind?.p_value;

  return (
    <div className="stats-v2">
      {/* Hero metrics */}
      <section className="stats-hero-grid" aria-label="Headline findings">
        <StatsHeroCard
          variant="rose"
          value={fmtPct(meanOverall, 2)}
          label="Average Pink Tax"
          subtext="Mean female-vs-male unit price gap, averaged across pairs"
        />
        <StatsHeroCard
          variant="rose"
          value={String(data?.meta?.n_rows ?? dist.n ?? "—")}
          label="Matched Product Pairs"
          subtext={`${hyd.distribution?.n ?? "—"} Hyderabad · ${tok.distribution?.n ?? "—"} Tokyo`}
        />
        <StatsHeroCard
          variant="rose"
          value={`Tokyo ${fmtPct(tok.distribution?.mean, 2)}`}
          label={`vs Hyderabad ${fmtPct(hyd.distribution?.mean, 2)}`}
          subtext="Mean female-vs-male unit price gap by city"
        />
        <StatsHeroCard
          variant="rose"
          value={topCatLabel}
          label="Highest Pink Tax Category"
          subtext={topCatSubtext}
        />
      </section>

      {/* Distribution overview */}
      <section className="panel stats-card">
        <header className="stats-card-head">
          <h2>Distribution of pink tax % (all pairs)</h2>
          <p className="stats-card-sub">
            Heavily right-skewed: median is exactly zero, but the mean is pulled up by a long
            right tail of high-premium pairs.
          </p>
        </header>
        <div className="stats-stat-grid">
          <Stat label="Mean" value={fmtPct(dist.mean, 2)} />
          <Stat label="Median" value={fmtPct(dist.median, 2)} />
          <Stat label="Std. dev." value={`${fmtNum(dist.std, 1)} pp`} />
          <Stat label="Min / Max" value={`${fmtPct(dist.min, 1)} / ${fmtPct(dist.max, 1)}`} />
          <Stat label="IQR (Q1–Q3)" value={`${fmtPct(dist.q1, 1)} → ${fmtPct(dist.q3, 1)}`} />
          <Stat label="P10 / P90" value={`${fmtPct(dist.p10, 1)} / ${fmtPct(dist.p90, 1)}`} />
          <Stat
            label="Direction split"
            value={`${fmtPct((direction.share_women_pay_more ?? 0) * 100, 1)} W>M · ${fmtPct(
              (direction.share_men_pay_more ?? 0) * 100, 1
            )} M>W · ${fmtPct((direction.share_parity ?? 0) * 100, 1)} parity`}
          />
        </div>
      </section>

      {/* Top-5 category bar chart */}
      <section className="panel stats-card">
        <header className="stats-card-head">
          <h2>Average Pink Tax by Product Category (top 5)</h2>
          <p className="stats-card-sub">
            Top five categories by mean female-vs-male price-per-unit gap.
          </p>
        </header>
        <CategoryBarChart data={top5Cats} />
      </section>

      {/* City comparison enhanced */}
      <section className="panel stats-card">
        <header className="stats-card-head">
          <h2>City comparison</h2>
          <p className="stats-card-sub">
            Per-city distribution, one-sample t-test vs zero, and direction breakdown
            (Section 5.2 of the paper).
          </p>
        </header>
        <div className="stats-city-grid">
          <CityStatCard
            name="Hyderabad"
            color="#1d4ed8"
            scale={cityCardScale}
            stats={hyd}
          />
          <CityStatCard
            name="Tokyo"
            color="#db2777"
            scale={cityCardScale}
            stats={tok}
          />
        </div>
        <p className="stats-city-note">
          Tokyo&apos;s mean pink tax is statistically distinguishable from zero
          (p = {fmtP(tok.one_sample_t_vs_zero?.p_value)}); Hyderabad&apos;s is not
          (p = {fmtP(hyd.one_sample_t_vs_zero?.p_value)}). Welch t-test comparing the two cities
          is not significant (p = {fmtP(cityP)}), reflecting high within-city variance.
        </p>
      </section>

      {/* Ingredient overlap buckets — Table 4: the headline finding */}
      <section className="panel stats-card stats-headline-card">
        <header className="stats-card-head">
          <h2>Pink tax by ingredient overlap (Table 4)</h2>
          <p className="stats-card-sub">
            The premium is <strong>highest</strong> among the most chemically similar products,
            contradicting the cost-based prediction of Moshary et al. (2023). This is the central
            theoretical finding of the paper.
          </p>
        </header>
        <CategoryBarChart data={overlapBars} colors={["#7c3aed", "#a855f7", "#c084fc", "#ec4899"]} />
        <table className="stats-table">
          <thead>
            <tr>
              <th>Ingredient overlap</th>
              <th>n</th>
              <th>Mean pink tax %</th>
              <th>Median</th>
            </tr>
          </thead>
          <tbody>
            {overlapBuckets.map((b) => (
              <tr key={b.bucket}>
                <td>{b.bucket}</td>
                <td>{b.n}</td>
                <td className={Number(b.mean_pink_tax_pct) > 0 ? "pos" : "neg"}>
                  {fmtPpt(b.mean_pink_tax_pct, 2)}
                </td>
                <td>{fmtPpt(b.median_pink_tax_pct, 2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* Cross-city category difference (Figure 3) */}
      {cityDiffTop.length > 0 && (
        <section className="panel stats-card">
          <header className="stats-card-head">
            <h2>Cross-city category difference (Tokyo − Hyderabad)</h2>
            <p className="stats-card-sub">
              Positive bars indicate Tokyo&apos;s premium exceeds Hyderabad&apos;s; negative bars the reverse.
              Top categories by absolute gap shown.
            </p>
          </header>
          <CategoryBarChart data={cityDiffTop} />
        </section>
      )}

      {/* Full Table 3 — pink tax by category */}
      <section className="panel stats-card">
        <header className="stats-card-head">
          <h2>Pink tax by product category (Table 3)</h2>
          <p className="stats-card-sub">
            One-sample t-tests against zero for every category. * p&lt;0.1, ** p&lt;0.05, *** p&lt;0.001.
          </p>
        </header>
        <div className="stats-table-scroll">
          <table className="stats-table">
            <thead>
              <tr>
                <th>Category</th>
                <th>n</th>
                <th>Mean pink tax %</th>
                <th>SD (pp)</th>
                <th>p-value</th>
              </tr>
            </thead>
            <tbody>
              {catTable.map((c) => (
                <tr key={c.category}>
                  <td>{c.category}</td>
                  <td>{c.n}</td>
                  <td className={Number(c.mean_pink_tax_pct) > 0 ? "pos" : "neg"}>
                    {fmtPpt(c.mean_pink_tax_pct, 1)}
                  </td>
                  <td>{fmtNum(c.std_pink_tax_pct, 1)}</td>
                  <td>
                    {fmtP(c.p_value)}
                    <span className="stats-stars">{pStars(c.p_value)}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Retailer summary — Section 5.5 */}
      {retailerSummary.length > 0 && (
        <section className="panel stats-card">
          <header className="stats-card-head">
            <h2>Retailer comparison (Section 5.5)</h2>
            <p className="stats-card-sub">
              Mean, median, and P90 of pink tax % by platform. Amazon.co.jp leads; Matsumoto Kiyoshi
              shows near-zero gaps throughout.
            </p>
          </header>
          <div className="stats-table-scroll">
            <table className="stats-table">
              <thead>
                <tr>
                  <th>Retailer</th>
                  <th>n</th>
                  <th>Mean %</th>
                  <th>Median %</th>
                  <th>P90 %</th>
                  <th>W&gt;M share</th>
                </tr>
              </thead>
              <tbody>
                {retailerSummary.map((r) => (
                  <tr key={r.retailer}>
                    <td>{r.retailer}</td>
                    <td>{r.n}</td>
                    <td className={r.mean > 0 ? "pos" : "neg"}>{fmtPpt(r.mean, 2)}</td>
                    <td>{fmtPpt(r.median, 2)}</td>
                    <td>{fmtPpt(r.p90, 1)}</td>
                    <td>{fmtPct((r.share_women_pay_more ?? 0) * 100, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Brand summary — Section 5.4 */}
      {brandSummary.length > 0 && (
        <section className="panel stats-card">
          <header className="stats-card-head">
            <h2>Brand-level patterns (Section 5.4, n ≥ 5)</h2>
            <p className="stats-card-sub">
              Mean pink tax by brand, ranked. Brand-level pricing strategy is at least as important
              as market-level dynamics.
            </p>
          </header>
          <div className="stats-table-scroll">
            <table className="stats-table">
              <thead>
                <tr>
                  <th>Brand</th>
                  <th>n</th>
                  <th>Mean %</th>
                  <th>Median %</th>
                  <th>SD</th>
                </tr>
              </thead>
              <tbody>
                {brandSummary.map((b) => (
                  <tr key={b.brand}>
                    <td>{b.brand}</td>
                    <td>{b.n}</td>
                    <td className={b.mean > 0 ? "pos" : "neg"}>{fmtPpt(b.mean, 2)}</td>
                    <td>{fmtPpt(b.median, 2)}</td>
                    <td>{fmtNum(b.std, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Regression Table 5 — M1–M4 */}
      <section className="panel stats-card stats-regression-card">
        <header className="stats-card-head">
          <h2>OLS regression results (Table 5)</h2>
          <p className="stats-card-sub">
            Pink tax % as outcome. HC3 robust SEs in parentheses. * p&lt;0.1, ** p&lt;0.05, *** p&lt;0.001.
            The Tokyo coefficient grows as controls are added: compositional differences in category and
            retailer mix had been suppressing the underlying city differential.
          </p>
        </header>
        <div className="stats-table-scroll">
          <table className="stats-table stats-table-regression">
            <thead>
              <tr>
                <th></th>
                <th>M1<br />City only</th>
                <th>M2<br />+ Category</th>
                <th>M3<br />+ Retailer</th>
                <th>M4<br />+ Ingredients</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Tokyo (vs Hyderabad)</td>
                <RegCell term={m1?.city_tokyo} />
                <RegCell term={m2?.city_tokyo} />
                <RegCell term={m3?.city_tokyo} />
                <RegCell term={m4Paper?.city_tokyo} />
              </tr>
              <tr>
                <td>Ingredient overlap %</td>
                <td>—</td>
                <td>—</td>
                <td>—</td>
                <RegCell term={m4Paper?.ingredient_overlap_pct} />
              </tr>
              <tr className="stats-table-divider">
                <td>Category FEs</td>
                <td>No</td><td>Yes</td><td>Yes</td><td>Yes</td>
              </tr>
              <tr>
                <td>Retailer FEs</td>
                <td>No</td><td>No</td><td>Yes</td><td>Yes</td>
              </tr>
              <tr>
                <td>R²</td>
                <td>{fmtNum(m1?.rsquared, 3)}</td>
                <td>{fmtNum(m2?.rsquared, 3)}</td>
                <td>{fmtNum(m3?.rsquared, 3)}</td>
                <td>{fmtNum(m4Paper?.rsquared, 3)}</td>
              </tr>
              <tr>
                <td>N</td>
                <td>{Math.round(m1?.nobs ?? 0)}</td>
                <td>{Math.round(m2?.nobs ?? 0)}</td>
                <td>{Math.round(m3?.nobs ?? 0)}</td>
                <td>{Math.round(m4Paper?.nobs ?? 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      {/* Plain-English regression takeaways */}
      <section className="panel stats-card stats-regression-card">
        <header className="stats-card-head">
          <h2>What predicts the pink tax?</h2>
          <p className="stats-card-sub">
            Summary of the regressions above (Section 6.3 of the paper).
          </p>
        </header>
        <ul className="stats-finding-list">
          <li>
            <span className="stats-finding-label">City effect</span>
            <span className="stats-finding-text">
              Tokyo is associated with{" "}
              <strong>{fmtPpt(m3?.city_tokyo?.coef, 1)} percentage points</strong> more pink tax than
              Hyderabad, controlling for category and retailer (p = {fmtP(m3?.city_tokyo?.p_value)}).
            </span>
          </li>
          <li>
            <span className="stats-finding-label">Price level</span>
            <span className="stats-finding-text">
              Higher-priced products tend to show a smaller pink tax (log mean PPU coef{" "}
              {fmtNum(m6?.log_mean_ppu?.coef, 2)}, p = {fmtP(m6?.log_mean_ppu?.p_value)}).
            </span>
          </li>
          <li>
            <span className="stats-finding-label">Product similarity</span>
            <span className="stats-finding-text">
              Ingredient overlap does not significantly predict pink tax size in M4
              (coef {fmtNum(m4Paper?.ingredient_overlap_pct?.coef, 2)},
              p = {fmtP(m4Paper?.ingredient_overlap_pct?.p_value)}). The bucketed analysis above
              tells a stronger story than the linear coefficient.
            </span>
          </li>
        </ul>
      </section>

      {/* Cleaning funnel + size ratio */}
      <section className="panel stats-card stats-twocol">
        <div>
          <header className="stats-card-head">
            <h2>Cleaning funnel (Table 2)</h2>
            <p className="stats-card-sub">
              Sequential filters that produced the 278-pair cleaned dataset.
            </p>
          </header>
          <table className="stats-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Operation</th>
                <th>In</th>
                <th>Out</th>
                <th>Removed</th>
              </tr>
            </thead>
            <tbody>
              {cleaningFunnel.map((s) => (
                <tr key={s.step}>
                  <td>{s.step}</td>
                  <td>{s.operation}</td>
                  <td>{s.n_in}</td>
                  <td>{s.n_out}</td>
                  <td>{s.removed}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div>
          <header className="stats-card-head">
            <h2>Pack-size matching</h2>
            <p className="stats-card-sub">
              Most pairs share an exact package size, so unit-price standardisation reduces to a raw
              price ratio.
            </p>
          </header>
          <div className="stats-stat-grid">
            <Stat
              label="Exact size match"
              value={fmtPct((sizeRatio.share_exact_match ?? 0) * 100, 1)}
            />
            <Stat
              label="Within ±10%"
              value={fmtPct((sizeRatio.share_within_10pct ?? 0) * 100, 1)}
            />
            <Stat
              label="Outside ±10%"
              value={fmtPct((sizeRatio.share_outside_10pct ?? 0) * 100, 1)}
            />
            <Stat label="Pairs analysed" value={String(sizeRatio.n ?? "—")} />
          </div>
        </div>
      </section>

      {/* Conclusion banner */}
      <section className="stats-banner" role="note">
        <p>
          Across <strong>{data?.meta?.n_rows ?? 278}</strong> same-brand product pairs in Hyderabad and Tokyo,
          female Stock Keeping Units (SKUs) cost <strong>{fmtPct(meanOverall, 2)} more per unit on average</strong> — a gap that is
          statistically significant (p = {fmtP(oneSampleP)}) and most pronounced in personal-care
          categories like <strong>{topCat?.category ?? "Hand Wash"}</strong> and{" "}
          <strong>{catTable[1]?.category ?? "Toothpaste"}</strong>. The pink tax is{" "}
          <strong>highest among near-identical formulations</strong> (90–100% ingredient overlap),
          which challenges cost-based explanations and points to branding as the primary driver.
        </p>
      </section>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div className="stats-stat">
      <div className="stats-stat-value">{value}</div>
      <div className="stats-stat-label">{label}</div>
    </div>
  );
}

function CityStatCard({ name, color, scale, stats }) {
  const mean = stats?.distribution?.mean;
  const t = stats?.one_sample_t_vs_zero ?? {};
  const dir = stats?.direction ?? {};
  const widthPct = mean !== undefined && mean !== null
    ? Math.max(6, Math.min(100, Math.round((Math.abs(Number(mean)) / scale) * 100)))
    : 0;
  return (
    <article className="stats-city-card">
      <div className="stats-city-dot" style={{ background: color }} aria-hidden="true" />
      <div className="stats-city-value">{fmtPct(mean, 2)}</div>
      <div className="stats-city-name">{name}</div>
      <div className="stats-city-bar-track">
        <div className="stats-city-bar-fill" style={{ width: `${widthPct}%`, background: color }} />
      </div>
      <dl className="stats-city-stats">
        <dt>One-sample t vs 0</dt>
        <dd>
          t = {fmtNum(t.statistic, 2)}, p = {fmtP(t.p_value)}
        </dd>
        <dt>95% CI for mean</dt>
        <dd>
          [{fmtPct(t.ci_lower, 1)}, {fmtPct(t.ci_upper, 1)}]
        </dd>
        <dt>Direction (W&gt;M / M&gt;W / parity)</dt>
        <dd>
          {fmtPct((dir.share_women_pay_more ?? 0) * 100, 1)} ·{" "}
          {fmtPct((dir.share_men_pay_more ?? 0) * 100, 1)} ·{" "}
          {fmtPct((dir.share_parity ?? 0) * 100, 1)}
        </dd>
      </dl>
    </article>
  );
}

function RegCell({ term }) {
  if (!term) return <td>—</td>;
  return (
    <td>
      <div className="reg-coef">
        {fmtPpt(term.coef, 2)}
        <span className="stats-stars">{pStars(term.p_value)}</span>
      </div>
      <div className="reg-se">({fmtNum(term.std_err, 2)})</div>
    </td>
  );
}

function niceTicks(minV, maxV, count = 7) {
  if (!Number.isFinite(minV) || !Number.isFinite(maxV)) return [0];
  if (minV === maxV) return [minV];
  const span = Math.abs(maxV - minV);
  const raw = span / Math.max(2, count - 1);
  const mag = 10 ** Math.floor(Math.log10(raw));
  const norm = raw / mag;
  let step = 1 * mag;
  if (norm > 1.5) step = 2 * mag;
  if (norm > 3.5) step = 5 * mag;
  if (norm > 7.5) step = 10 * mag;
  const start = Math.floor(minV / step) * step;
  const end = Math.ceil(maxV / step) * step;
  const out = [];
  for (let v = start; v <= end + step / 2; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

function gaussianKernel(u) {
  return Math.exp(-0.5 * u * u) / Math.sqrt(2 * Math.PI);
}

function kdeDensity(values, ySamples) {
  if (!values.length) return ySamples.map((y) => ({ y, d: 0 }));
  const n = values.length;
  const mean = avg(values);
  const variance = avg(values.map((v) => (v - mean) ** 2));
  const std = Math.sqrt(Math.max(variance, 1e-12));
  const minV = Math.min(...values);
  const maxV = Math.max(...values);
  const fallbackBw = Math.max((maxV - minV) / 16, 4);
  const bw = Math.max(1.06 * std * n ** (-1 / 5), fallbackBw);
  return ySamples.map((y) => {
    const d = values.reduce((acc, v) => acc + gaussianKernel((y - v) / bw), 0) / (n * bw);
    return { y, d };
  });
}

function LiveViolinPlot({ categories, yMin, yMax, height = 520 }) {
  const wrapRef = useRef(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return undefined;
    const update = () => setWidth(el.clientWidth || 0);
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    window.addEventListener("resize", update);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", update);
    };
  }, []);

  const chart = useMemo(() => {
    if (!width || !categories.length) return null;
    const margin = { top: 20, right: 26, bottom: 96, left: 72 };
    const innerW = Math.max(120, width - margin.left - margin.right);
    const innerH = Math.max(160, height - margin.top - margin.bottom);
    const yLo = Number.isFinite(yMin) ? yMin : -100;
    const yHi = Number.isFinite(yMax) ? yMax : 100;
    const ySpan = Math.max(1, yHi - yLo);
    const yScale = (v) => margin.top + ((yHi - v) / ySpan) * innerH;
    const step = innerW / Math.max(1, categories.length);
    const xCenter = (i) => margin.left + step * (i + 0.5);
    const maxHalfWidth = step * 0.38;
    const sampleN = 120;
    const ySamples = Array.from({ length: sampleN }, (_, i) => yLo + (i * ySpan) / (sampleN - 1));
    const ticks = niceTicks(yLo, yHi, 7).filter((t) => t >= yLo - 1e-6 && t <= yHi + 1e-6);

    const violins = categories.map((cat, i) => {
      const dens = kdeDensity(cat.values, ySamples);
      const maxD = Math.max(...dens.map((p) => p.d), 1e-9);
      const center = xCenter(i);
      const rightPts = dens.map((p) => {
        const w = (p.d / maxD) * maxHalfWidth;
        return [center + w, yScale(p.y)];
      });
      const leftPts = dens
        .slice()
        .reverse()
        .map((p) => {
          const w = (p.d / maxD) * maxHalfWidth;
          return [center - w, yScale(p.y)];
        });
      const points = [...rightPts, ...leftPts];
      const path = points.map((pt, idx) => `${idx === 0 ? "M" : "L"} ${pt[0].toFixed(2)} ${pt[1].toFixed(2)}`).join(" ") + " Z";
      return { ...cat, center, path };
    });

    return { margin, innerW, innerH, yScale, xCenter, ticks, violins };
  }, [categories, yMax, yMin, width, height]);

  return (
    <div ref={wrapRef} className="violin-live-shell">
      {chart ? (
        <svg width={width} height={height} role="img" aria-label="Top category violin distribution">
          <rect x="0" y="0" width={width} height={height} fill="transparent" />

          {chart.ticks.map((t) => {
            const y = chart.yScale(t);
            return (
              <g key={`tick-${t}`}>
                <line
                  x1={chart.margin.left}
                  y1={y}
                  x2={chart.margin.left + chart.innerW}
                  y2={y}
                  stroke="#d1d5db"
                  strokeDasharray="4 4"
                />
                <text x={chart.margin.left - 10} y={y + 4} textAnchor="end" fontSize="12" fill="#374151">
                  {Number(t.toFixed(0))}
                </text>
              </g>
            );
          })}

          <line
            x1={chart.margin.left}
            y1={chart.yScale(0)}
            x2={chart.margin.left + chart.innerW}
            y2={chart.yScale(0)}
            stroke="#475569"
            strokeDasharray="5 5"
            strokeWidth="1.2"
          />

          {chart.violins.map((row) => (
            <g key={`vio-${row.label}`}>
              <path d={row.path} fill={row.color} fillOpacity="0.48" stroke="#9d174d" strokeWidth="1" />
              <line
                x1={row.center - chart.innerW * 0.065}
                y1={chart.yScale(row.q1)}
                x2={row.center + chart.innerW * 0.065}
                y2={chart.yScale(row.q1)}
                stroke="#334155"
                strokeDasharray="3 2"
                strokeWidth="1"
              />
              <line
                x1={row.center - chart.innerW * 0.065}
                y1={chart.yScale(row.median)}
                x2={row.center + chart.innerW * 0.065}
                y2={chart.yScale(row.median)}
                stroke="#1e293b"
                strokeDasharray="3 2"
                strokeWidth="1"
              />
              <line
                x1={row.center - chart.innerW * 0.065}
                y1={chart.yScale(row.q3)}
                x2={row.center + chart.innerW * 0.065}
                y2={chart.yScale(row.q3)}
                stroke="#334155"
                strokeDasharray="3 2"
                strokeWidth="1"
              />
            </g>
          ))}

          <line
            x1={chart.margin.left}
            y1={chart.margin.top}
            x2={chart.margin.left}
            y2={chart.margin.top + chart.innerH}
            stroke="#6b7280"
            strokeWidth="1.2"
          />
          <line
            x1={chart.margin.left}
            y1={chart.margin.top + chart.innerH}
            x2={chart.margin.left + chart.innerW}
            y2={chart.margin.top + chart.innerH}
            stroke="#6b7280"
            strokeWidth="1.2"
          />

          {chart.violins.map((row, i) => (
            <g key={`xlab-${row.label}`}>
              <line
                x1={chart.xCenter(i)}
                y1={chart.margin.top + chart.innerH}
                x2={chart.xCenter(i)}
                y2={chart.margin.top + chart.innerH + 6}
                stroke="#6b7280"
                strokeWidth="1"
              />
              <text
                x={chart.xCenter(i)}
                y={chart.margin.top + chart.innerH + 30}
                textAnchor="end"
                fontSize="12"
                fill="#4b5563"
                transform={`rotate(-24 ${chart.xCenter(i)} ${chart.margin.top + chart.innerH + 30})`}
              >
                {shortLabel(row.label, 20)}
              </text>
            </g>
          ))}

          <text
            x={chart.margin.left - 46}
            y={chart.margin.top + chart.innerH / 2}
            textAnchor="middle"
            transform={`rotate(-90 ${chart.margin.left - 46} ${chart.margin.top + chart.innerH / 2})`}
            fontSize="18"
            fill="#4b5563"
          >
            Pink Tax %
          </text>
          <text
            x={chart.margin.left + chart.innerW / 2}
            y={height - 18}
            textAnchor="middle"
            fontSize="12"
            fill="#4b5563"
          >
            Category
          </text>
        </svg>
      ) : (
        <div className="violin-empty">No data for violin chart</div>
      )}
    </div>
  );
}

function toBoxBandRows(rows, keyField = "key") {
  return rows.map((r) => ({
    ...r,
    label: r[keyField],
    iqrBase: Number(r.q1),
    iqrSpan: Number((r.q3 - r.q1).toFixed(2))
  }));
}

function heatColor(value, maxAbs) {
  const v = toNum(value);
  if (v === null) return "#f8fafc";
  const ratio = Math.min(1, Math.abs(v) / Math.max(1, maxAbs));
  if (v >= 0) return `rgba(225, 29, 72, ${0.14 + 0.56 * ratio})`;
  return `rgba(29, 78, 216, ${0.14 + 0.56 * ratio})`;
}

const DIVERGING_PALETTES = {
  category: { pos: "#b45309", neg: "#1d4ed8", zero: "#64748b" },
  brand: { pos: "#be185d", neg: "#0f766e", zero: "#64748b" },
  cityDiff: { pos: "#dc2626", neg: "#2563eb", zero: "#64748b" }
};
const VIOLIN_COLORS = ["#e7d3d4", "#e4b8bf", "#d98ba9", "#cc5d9b", "#a82686", "#7c147a"];
const PROJECT_DATE_START = "2026-03-08";
const PROJECT_DATE_END = "2026-03-20";
const PROJECT_WINDOW_DAYS = 12;

function divergingBarColor(value, palette = DIVERGING_PALETTES.category) {
  const v = toNum(value);
  if (v === null || v === 0) return palette.zero;
  return v > 0 ? palette.pos : palette.neg;
}

export default function App() {
  const sidebarRef = useRef(null);
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [city, setCity] = useState("ALL");
  const [retailer, setRetailer] = useState("ALL");
  const [category, setCategory] = useState("ALL");
  const [confidence, setConfidence] = useState("ALL");
  const [minMatchQuality, setMinMatchQuality] = useState(0);
  const [histBins, setHistBins] = useState(28);
  const [extremeRowsCount, setExtremeRowsCount] = useState(15);
  const [showValueLabels, setShowValueLabels] = useState(false);
  const [showSidebarHint, setShowSidebarHint] = useState(false);
  const [mainTab, setMainTab] = useState("explore");
  const [regressionPayload, setRegressionPayload] = useState(null);

  useEffect(() => {
    const base = import.meta.env.BASE_URL || "/";
    const urls = [
      `${base}data/regression_summary.json`,
      "/data/regression_summary.json",
      "./data/regression_summary.json"
    ];
    (async () => {
      for (const url of urls) {
        try {
          const res = await fetch(url, { cache: "no-store" });
          if (!res.ok) continue;
          const data = await res.json();
          if (data?.descriptive || data?.models) {
            setRegressionPayload(data);
            return;
          }
        } catch {
          /* optional artifact */
        }
      }
    })();
  }, []);

  useEffect(() => {
    const base = import.meta.env.BASE_URL || "/";
    const candidates = [
      `${base}data/eda_summary.json`,
      "/data/eda_summary.json",
      "./data/eda_summary.json"
    ];

    async function loadData() {
      setLoading(true);
      setLoadError("");
      let lastError = "Unknown data loading error";

      for (const url of candidates) {
        try {
          const res = await fetch(url, { cache: "no-store" });
          if (!res.ok) {
            lastError = `HTTP ${res.status} for ${url}`;
            continue;
          }
          const data = await res.json();
          if (!Array.isArray(data?.records) || data.records.length === 0) {
            lastError = `Loaded ${url} but records are empty`;
            continue;
          }
          setPayload(data);
          setLoading(false);
          return;
        } catch (err) {
          lastError = `${url} -> ${String(err)}`;
        }
      }

      setPayload(null);
      setLoadError(lastError);
      setLoading(false);
    }

    loadData();
  }, []);

  useEffect(() => {
    const el = sidebarRef.current;
    if (!el) return undefined;

    const refreshHint = () => {
      const hasOverflow = el.scrollHeight > el.clientHeight + 2;
      const nearTop = el.scrollTop < 6;
      setShowSidebarHint(hasOverflow && nearTop);
    };

    refreshHint();
    el.addEventListener("scroll", refreshHint, { passive: true });
    window.addEventListener("resize", refreshHint);

    let observer = null;
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(refreshHint);
      observer.observe(el);
    }

    return () => {
      el.removeEventListener("scroll", refreshHint);
      window.removeEventListener("resize", refreshHint);
      observer?.disconnect();
    };
  }, [loading]);

  const records = payload?.records ?? [];
  const totalRows = records.length;
  const minMatchQualityBound = useMemo(() => {
    const values = records
      .map((r) => toNum(r.match_quality))
      .filter((v) => v !== null);
    if (!values.length) return 0;
    return Math.min(...values);
  }, [records]);
  const maxMatchQualityBound = useMemo(() => {
    const values = records
      .map((r) => toNum(r.match_quality))
      .filter((v) => v !== null);
    if (!values.length) return 5;
    return Math.max(...values);
  }, [records]);
  const confidenceOptions = useMemo(
    () => [...new Set(records.map((r) => String(r.confidence ?? "")).filter(Boolean))].sort(),
    [records]
  );

  useEffect(() => {
    if (!records.length) return;
    setMinMatchQuality((prev) => (prev < minMatchQualityBound ? minMatchQualityBound : prev));
  }, [records, minMatchQualityBound]);

  const filtered = useMemo(() => {
    return records.filter((r) => {
      if (city !== "ALL" && r.city !== city) return false;
      if (retailer !== "ALL" && r.retailer !== retailer) return false;
      if (category !== "ALL" && r.category !== category) return false;
      if (confidence !== "ALL" && String(r.confidence ?? "") !== confidence) return false;
      if (minMatchQuality > 0) {
        const mq = toNum(r.match_quality);
        if (mq === null || mq < minMatchQuality) return false;
      }
      return true;
    });
  }, [records, city, retailer, category, confidence, minMatchQuality]);

  const scaled = (height) => height;

  const pinkVals = useMemo(
    () => filtered.map((r) => toNum(r.pink_tax_pct)).filter((v) => v !== null),
    [filtered]
  );

  const overall = useMemo(() => {
    const pos = pinkVals.filter((v) => v > 0).length;
    const neg = pinkVals.filter((v) => v < 0).length;
    const zero = pinkVals.filter((v) => v === 0).length;
    return {
      rows: filtered.length,
      pairs: new Set(filtered.map((r) => r.pair_code)).size,
      mean: avg(pinkVals),
      median: median(pinkVals),
      q1: quantile(pinkVals, 0.25),
      q3: quantile(pinkVals, 0.75),
      p40: quantile(pinkVals, 0.40),
      p60: quantile(pinkVals, 0.60),
      p10: quantile(pinkVals, 0.10),
      p90: quantile(pinkVals, 0.90),
      posShare: pinkVals.length ? (pos / pinkVals.length) * 100 : 0,
      negShare: pinkVals.length ? (neg / pinkVals.length) * 100 : 0,
      zeroShare: pinkVals.length ? (zero / pinkVals.length) * 100 : 0
    };
  }, [filtered, pinkVals]);
  const filteredShare = useMemo(
    () => (totalRows ? (filtered.length / totalRows) * 100 : 0),
    [filtered.length, totalRows]
  );
  const descriptiveStats = useMemo(() => {
    const rows = filtered.length;
    const safePct = (num, den) => (den ? (num / den) * 100 : 0);
    const toMapCounts = (key) => {
      const m = new Map();
      for (const r of filtered) {
        const v = String(r[key] ?? "").trim();
        if (!v) continue;
        m.set(v, (m.get(v) || 0) + 1);
      }
      return m;
    };

    const cityCounts = toMapCounts("city");
    const retailerCounts = toMapCounts("retailer");
    const categoryCounts = toMapCounts("category");
    const brandCounts = toMapCounts("brand");

    const largestShare = (m) => {
      if (!m.size || !rows) return 0;
      return Math.max(...m.values()) / rows;
    };

    const dateStart = PROJECT_DATE_START;
    const dateEnd = PROJECT_DATE_END;
    const daySpan = PROJECT_WINDOW_DAYS;

    const mqVals = filtered.map((r) => toNum(r.match_quality)).filter((v) => v !== null);
    const mqHigh = mqVals.filter((v) => v >= 4).length;
    const mqLow = mqVals.filter((v) => v <= 2).length;

    const highConf = filtered.filter((r) => String(r.confidence ?? "").toUpperCase() === "HIGH").length;
    const medConf = filtered.filter((r) => String(r.confidence ?? "").toUpperCase() === "MED").length;
    const lowConf = filtered.filter((r) => String(r.confidence ?? "").toUpperCase() === "LOW").length;

    const ingredientVals = filtered.map((r) => toNum(r.ingredient_overlap_pct)).filter((v) => v !== null);
    const sizePairs = filtered
      .map((r) => {
        const fs = toNum(r.female_size);
        const ms = toNum(r.male_size);
        if (fs === null || ms === null || ms <= 0) return null;
        return fs / ms;
      })
      .filter((v) => v !== null);
    const sameSize = sizePairs.filter((v) => Math.abs(v - 1) < 1e-9).length;
    const within10 = sizePairs.filter((v) => Math.abs(v - 1) <= 0.1).length;
    const within30 = sizePairs.filter((v) => Math.abs(v - 1) <= 0.3).length;

    const nearZero1 = pinkVals.filter((v) => Math.abs(v) <= 1).length;
    const nearZero5 = pinkVals.filter((v) => Math.abs(v) <= 5).length;
    const gt100 = pinkVals.filter((v) => v > 100).length;
    const ltMinus50 = pinkVals.filter((v) => v < -50).length;
    const trimmed = trimmedMean(pinkVals, 0.1);
    const mad = median(pinkVals.map((v) => Math.abs(v - overall.median)));

    const completeRows = filtered.filter((r) => {
      const req = [
        r.pair_code, r.city, r.retailer, r.category, r.brand,
        r.female_price_local, r.male_price_local, r.female_size, r.male_size, r.pink_tax_pct
      ];
      return req.every((v) => String(v ?? "").trim() !== "");
    }).length;
    const ppuRows = filtered.filter((r) => {
      const f = toNum(r.female_ppu_local);
      const m = toNum(r.male_ppu_local);
      return f !== null && m !== null && f > 0 && m > 0;
    }).length;

    return {
      dateStart,
      dateEnd,
      daySpan,
      cityCount: cityCounts.size,
      retailerCount: retailerCounts.size,
      categoryCount: categoryCounts.size,
      brandCount: brandCounts.size,
      topCityShare: largestShare(cityCounts) * 100,
      topRetailerShare: largestShare(retailerCounts) * 100,
      topCategoryShare: largestShare(categoryCounts) * 100,
      mqMedian: median(mqVals),
      mqHighShare: safePct(mqHigh, mqVals.length),
      mqLowShare: safePct(mqLow, mqVals.length),
      confHighShare: safePct(highConf, rows),
      confMedShare: safePct(medConf, rows),
      confLowShare: safePct(lowConf, rows),
      ingredientCoverage: safePct(ingredientVals.length, rows),
      ingredientMedian: median(ingredientVals),
      ingredientIqr: quantile(ingredientVals, 0.75) - quantile(ingredientVals, 0.25),
      exactSizeShare: safePct(sameSize, sizePairs.length),
      within10Share: safePct(within10, sizePairs.length),
      within30Share: safePct(within30, sizePairs.length),
      stdev: stdDev(pinkVals),
      iqrWidth: overall.q3 - overall.q1,
      trimmedMean10: trimmed,
      mad,
      nearZero1Share: safePct(nearZero1, pinkVals.length),
      nearZero5Share: safePct(nearZero5, pinkVals.length),
      outlierHighShare: safePct(gt100, pinkVals.length),
      outlierLowShare: safePct(ltMinus50, pinkVals.length),
      posNegRatio: overall.negShare > 0 ? overall.posShare / overall.negShare : 0,
      completeShare: safePct(completeRows, rows),
      ppuShare: safePct(ppuRows, rows)
    };
  }, [filtered, pinkVals, overall]);

  const histData = useMemo(() => histogram(filtered, histBins), [filtered, histBins]);
  const retailerStats = useMemo(
    () => buildGroupStats(filtered, "retailer").sort((a, b) => b.n - a.n),
    [filtered]
  );
  const retailerBoxBand = useMemo(() => toBoxBandRows(retailerStats), [retailerStats]);
  const categoryStats = useMemo(
    () => buildGroupStats(filtered, "category").sort((a, b) => b.mean - a.mean),
    [filtered]
  );
  const brandStats = useMemo(
    () => buildGroupStats(filtered, "brand").sort((a, b) => b.mean - a.mean),
    [filtered]
  );
  const displayedCategoryStats = categoryStats;
  const displayedBrandStats = brandStats;
  const compactCategoryStats = useMemo(
    () =>
      [...displayedCategoryStats]
        .sort((a, b) => Math.abs(b.mean) - Math.abs(a.mean))
        .slice(0, 8),
    [displayedCategoryStats]
  );
  const compactBrandStats = useMemo(
    () =>
      [...displayedBrandStats]
        .sort((a, b) => Math.abs(b.mean) - Math.abs(a.mean))
        .slice(0, 8),
    [displayedBrandStats]
  );
  const compactCategoryHeight = useMemo(
    () => scaled(Math.max(300, compactCategoryStats.length * 40 + 10)),
    [compactCategoryStats.length]
  );
  const compactBrandHeight = useMemo(
    () => scaled(Math.max(300, compactBrandStats.length * 40 + 10)),
    [compactBrandStats.length]
  );
  const cityStats = useMemo(
    () => buildGroupStats(filtered, "city").sort((a, b) => b.n - a.n),
    [filtered]
  );
  const cityBoxBand = useMemo(() => toBoxBandRows(cityStats), [cityStats]);
  const cityStrip = useMemo(() => {
    const cities = [...new Set(filtered.map((r) => r.city).filter(Boolean))];
    const cityIdx = new Map(cities.map((c, i) => [c, i]));
    const out = [];
    let idx = 0;
    for (const r of filtered) {
      const v = toNum(r.pink_tax_pct);
      if (v === null) continue;
      const cx = cityIdx.get(r.city);
      if (cx === undefined) continue;
      const jitter = ((idx % 17) - 8) / 40;
      out.push({ x: cx + jitter, y: v, city: r.city });
      idx += 1;
    }
    return { points: out, labels: cities };
  }, [filtered]);
  const cityMedianDomain = useMemo(() => {
    if (!cityStats.length) return [-10, 10];
    const values = cityStats.flatMap((row) => [row.q1, row.median, row.q3, row.mean]);
    const minV = Math.min(...values);
    const maxV = Math.max(...values);
    const pad = Math.max(4, (maxV - minV) * 0.15);
    let low = Math.floor((minV - pad) / 5) * 5;
    let high = Math.ceil((maxV + pad) / 5) * 5;
    low = Math.min(low, 0);
    high = Math.max(high, 0);
    if (low === high) {
      low -= 5;
      high += 5;
    }
    return [low, high];
  }, [cityStats]);
  const directionByRetailer = useMemo(
    () =>
      retailerStats.map((r) => ({
        retailer: r.key,
        positive: r.positiveShare,
        negative: r.negativeShare,
        zero: r.zeroShare
      })),
    [retailerStats]
  );
  const overlapScatter = useMemo(
    () =>
      filtered
        .map((r) => ({
          ingredient_overlap_pct: toNum(r.ingredient_overlap_pct),
          pink_tax_pct: toNum(r.pink_tax_pct),
          retailer: r.retailer
        }))
        .filter((r) => r.ingredient_overlap_pct !== null && r.pink_tax_pct !== null),
    [filtered]
  );
  const directionByCity = useMemo(
    () =>
      cityStats.map((r) => ({
        city: r.key,
        positive: r.positiveShare,
        negative: r.negativeShare,
        zero: r.zeroShare
      })),
    [cityStats]
  );
  const sizeRatioData = useMemo(() => {
    const stats = { exact: 0, within10: 0, outside10: 0, total: 0 };
    for (const r of filtered) {
      const fs = toNum(r.female_size);
      const ms = toNum(r.male_size);
      if (fs === null || ms === null || ms <= 0) continue;
      const ratio = fs / ms;
      stats.total += 1;
      if (Math.abs(ratio - 1) < 1e-9) stats.exact += 1;
      else if (Math.abs(ratio - 1) <= 0.1) stats.within10 += 1;
      else stats.outside10 += 1;
    }
    if (!stats.total) return [];
    return [
      { group: "Exact same size", share: (stats.exact / stats.total) * 100 },
      { group: "Within ±10%", share: (stats.within10 / stats.total) * 100 },
      { group: "Outside ±10%", share: (stats.outside10 / stats.total) * 100 }
    ];
  }, [filtered]);
  const ppuScatterData = useMemo(
    () =>
      filtered
        .map((r) => {
          const femalePPU = toNum(r.female_ppu_local);
          const malePPU = toNum(r.male_ppu_local);
          if (femalePPU === null || malePPU === null || femalePPU <= 0 || malePPU <= 0) return null;
          return {
            city: r.city,
            retailer: r.retailer,
            category: r.category,
            femalePPU,
            malePPU,
            femaleLog: Math.log10(femalePPU),
            maleLog: Math.log10(malePPU)
          };
        })
        .filter(Boolean),
    [filtered]
  );
  const ppuScatterByCity = useMemo(
    () => ({
      Hyderabad: ppuScatterData.filter((r) => r.city === "Hyderabad"),
      Tokyo: ppuScatterData.filter((r) => r.city === "Tokyo")
    }),
    [ppuScatterData]
  );
  const ppuLogDomain = useMemo(() => {
    if (!ppuScatterData.length) return [0, 2];
    const vals = ppuScatterData.flatMap((r) => [r.femaleLog, r.maleLog]);
    const minV = Math.min(...vals);
    const maxV = Math.max(...vals);
    const pad = Math.max(0.08, (maxV - minV) * 0.08);
    return [Number((minV - pad).toFixed(2)), Number((maxV + pad).toFixed(2))];
  }, [ppuScatterData]);
  const topCategoriesForDistribution = useMemo(() => {
    const counts = new Map();
    for (const r of filtered) {
      const cat = r.category;
      if (!cat) continue;
      counts.set(cat, (counts.get(cat) || 0) + 1);
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([cat]) => cat);
  }, [filtered]);
  const topCategoryDistribution = useMemo(() => {
    const buckets = new Map(topCategoriesForDistribution.map((cat) => [cat, []]));
    const colorMap = new Map(topCategoriesForDistribution.map((cat, i) => [cat, VIOLIN_COLORS[i % VIOLIN_COLORS.length]]));
    for (const r of filtered) {
      const cat = r.category;
      if (!buckets.has(cat)) continue;
      const pink = toNum(r.pink_tax_pct);
      if (pink === null) continue;
      buckets.get(cat).push(pink);
    }
    const allVals = [...buckets.values()].flat();
    const yMinRaw = allVals.length ? Math.min(...allVals) : -10;
    const yMaxRaw = allVals.length ? Math.max(...allVals) : 10;
    const yPad = Math.max(8, (yMaxRaw - yMinRaw) * 0.1);
    const yMin = yMinRaw - yPad;
    const yMax = yMaxRaw + yPad;

    const categories = topCategoriesForDistribution
      .map((cat) => {
        const vals = buckets.get(cat) || [];
        if (!vals.length) return null;
        const sortedVals = [...vals].sort((a, b) => a - b);
        return {
          label: cat,
          values: sortedVals,
          q1: quantile(sortedVals, 0.25),
          median: quantile(sortedVals, 0.5),
          q3: quantile(sortedVals, 0.75),
          color: colorMap.get(cat),
          n: sortedVals.length
        };
      })
      .filter(Boolean);

    return { categories, yMin, yMax };
  }, [filtered, topCategoriesForDistribution]);
  const overlapBucketData = useMemo(() => {
    const bins = [
      { label: "0-50", min: 0, max: 50 },
      { label: "50-75", min: 50, max: 75 },
      { label: "75-90", min: 75, max: 90 },
      { label: "90-100", min: 90, max: 100 }
    ];
    return bins
      .map((b) => {
        const vals = filtered
          .map((r) => ({
            overlap: toNum(r.ingredient_overlap_pct),
            pink: toNum(r.pink_tax_pct)
          }))
          .filter((x) => x.overlap !== null && x.pink !== null)
          .filter((x) => (b.label === "0-50" ? x.overlap >= b.min && x.overlap <= b.max : x.overlap > b.min && x.overlap <= b.max))
          .map((x) => x.pink);
        return {
          bucket: b.label,
          mean: vals.length ? avg(vals) : 0,
          n: vals.length
        };
      })
      .filter((x) => x.n > 0);
  }, [filtered]);
  const cityMedianPPU = useMemo(() => {
    const bucket = new Map();
    for (const r of filtered) {
      const cityKey = r.city;
      if (!cityKey) continue;
      const f = toNum(r.female_ppu_local);
      const m = toNum(r.male_ppu_local);
      if (!bucket.has(cityKey)) bucket.set(cityKey, { city: cityKey, female: [], male: [] });
      const row = bucket.get(cityKey);
      if (f !== null && f > 0) row.female.push(f);
      if (m !== null && m > 0) row.male.push(m);
    }
    return [...bucket.values()].map((row) => ({
      city: row.city,
      femaleMedianPPU: row.female.length ? Number(median(row.female).toFixed(2)) : null,
      maleMedianPPU: row.male.length ? Number(median(row.male).toFixed(2)) : null
    }));
  }, [filtered]);
  const categoryCityDiff = useMemo(() => {
    const byCategory = new Map();
    for (const r of filtered) {
      const c = r.category;
      const cityKey = r.city;
      const v = toNum(r.pink_tax_pct);
      if (!c || !cityKey || v === null) continue;
      if (!byCategory.has(c)) byCategory.set(c, { Hyderabad: [], Tokyo: [] });
      const slot = byCategory.get(c);
      if (slot[cityKey]) slot[cityKey].push(v);
    }
    const out = [];
    for (const [category, vals] of byCategory.entries()) {
      if (!vals.Hyderabad.length || !vals.Tokyo.length) continue;
      const hyd = avg(vals.Hyderabad);
      const tky = avg(vals.Tokyo);
      out.push({
        category,
        diff: Number((tky - hyd).toFixed(2)),
        tokyoMean: Number(tky.toFixed(2)),
        hydMean: Number(hyd.toFixed(2))
      });
    }
    return out.sort((a, b) => a.diff - b.diff);
  }, [filtered]);
  const displayedCategoryCityDiff = categoryCityDiff;
  const cityCategoryHeat = useMemo(() => {
    const categories = [...new Set(filtered.map((r) => r.category).filter(Boolean))].sort();
    const cities = [...new Set(filtered.map((r) => r.city).filter(Boolean))].sort();
    const rows = categories.map((cat) => {
      const row = { category: cat };
      for (const cityName of cities) {
        const vals = filtered
          .filter((r) => r.category === cat && r.city === cityName)
          .map((r) => toNum(r.pink_tax_pct))
          .filter((v) => v !== null);
        row[cityName] = vals.length ? Number(avg(vals).toFixed(2)) : null;
      }
      return row;
    });
    const vals = rows.flatMap((r) => cities.map((c) => toNum(r[c])).filter((v) => v !== null));
    const maxAbs = vals.length ? Math.max(...vals.map((v) => Math.abs(v)), 1) : 1;
    return { rows, cities, maxAbs };
  }, [filtered]);
  const categoryDiffChartHeight = useMemo(
    () => scaled(Math.max(520, displayedCategoryCityDiff.length * 40)),
    [displayedCategoryCityDiff.length]
  );
  const extremes = useMemo(
    () =>
      filtered
        .map((r) => ({ ...r, pinkNum: toNum(r.pink_tax_pct) }))
        .filter((r) => r.pinkNum !== null)
        .sort((a, b) => Math.abs(b.pinkNum) - Math.abs(a.pinkNum))
        .slice(0, extremeRowsCount),
    [filtered, extremeRowsCount]
  );

  const filterLists = payload?.filters ?? { cities: [], retailers: [], categories: [] };

  if (loading) {
    return (
      <main className="loading-shell">
        <section className="loading-card">
          <div className="loading-spinner" />
          <h2>Loading Pink Tax Observatory</h2>
          <p>Preparing charts and summary metrics from the latest EDA artifacts.</p>
        </section>
      </main>
    );
  }

  if (loadError) {
    return (
      <main className="page">
        <section className="panel">
          <h2>Data load error</h2>
          <p>{loadError}</p>
          <p>
            Run: <code>python scripts/analysis/run_eda.py</code> (and optionally{" "}
            <code>python scripts/analysis/run_regression.py</code>) then restart the React dev server.
          </p>
        </section>
      </main>
    );
  }

  return (
    <main className="page">
      <aside ref={sidebarRef} className={`sidebar ${showSidebarHint ? "show-scroll-hint" : ""}`}>
        <header className="hero">
          <p className="eyebrow">Exploratory Data Analysis</p>
          <h1>Pink Tax Observatory</h1>
          <p className="subtitle">
            Academic dashboard for same-brand female vs male pricing patterns across Hyderabad and Tokyo.
          </p>
          <nav className="hero-tabs" aria-label="Main views">
            <button
              type="button"
              className={mainTab === "explore" ? "is-active" : ""}
              onClick={() => setMainTab("explore")}
            >
              EDA
            </button>
            <button
              type="button"
              className={mainTab === "stats" ? "is-active" : ""}
              onClick={() => setMainTab("stats")}
            >
              Conclusions
            </button>
          </nav>
        </header>

      <section className="panel panel-sidebar" id="dashboard-start">
        <h2>Filters</h2>
        <div className="filters">
          <label>
            City
            <select value={city} onChange={(e) => setCity(e.target.value)}>
              <option value="ALL">All</option>
              {filterLists.cities.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <label>
            Retailer
            <select value={retailer} onChange={(e) => setRetailer(e.target.value)}>
              <option value="ALL">All</option>
              {filterLists.retailers.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <label>
            Category
            <select value={category} onChange={(e) => setCategory(e.target.value)}>
              <option value="ALL">All</option>
              {filterLists.categories.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <label>
            Confidence
            <select value={confidence} onChange={(e) => setConfidence(e.target.value)}>
              <option value="ALL">All</option>
              {confidenceOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            onClick={() => {
              setCity("ALL");
              setRetailer("ALL");
              setCategory("ALL");
              setConfidence("ALL");
              setMinMatchQuality(minMatchQualityBound);
              setHistBins(28);
              setExtremeRowsCount(15);
              setShowValueLabels(false);
            }}
          >
            Reset All
          </button>
        </div>
      </section>

      <section className="panel panel-sidebar">
        <h2>Visualization Controls</h2>
        <div className="controls">
          <label>
            Histogram bins
            <input
              type="range"
              min="12"
              max="48"
              step="2"
              value={histBins}
              onChange={(e) => setHistBins(Number(e.target.value))}
            />
            <span className="control-value">{histBins}</span>
          </label>
          <label>
            Minimum Match Quality
            <input
              type="range"
              min={minMatchQualityBound}
              max={maxMatchQualityBound}
              step="1"
              value={minMatchQuality}
              onChange={(e) => setMinMatchQuality(Number(e.target.value))}
            />
            <span className="control-value">{minMatchQuality}</span>
          </label>
          <label>
            Extreme rows shown
            <input
              type="range"
              min="5"
              max="40"
              step="1"
              value={extremeRowsCount}
              onChange={(e) => setExtremeRowsCount(Number(e.target.value))}
            />
            <span className="control-value">{extremeRowsCount}</span>
          </label>
          <label className="control-check">
            Bar labels
            <span className="check-row">
              <input
                type="checkbox"
                checked={showValueLabels}
                onChange={(e) => setShowValueLabels(e.target.checked)}
              />
              Show numeric labels on bars
            </span>
          </label>
        </div>
      </section>
      </aside>

      <div className="content">
      {mainTab === "explore" && (
      <>
      <section className="panel read-guide">
        <h2>How To Read This Dashboard</h2>
        <div className="read-guide-grid">
          <div>
            <p className="chart-note">
              This dashboard compares prices for products marketed to women and men from the same brand.
              Every chart updates when you change filters for city, retailer, category, confidence, and match quality. The dataset used in this dashboard is a compilation of product pairs collected from Hyderabad and Tokyo across multiple retailers and categories, with match quality and confidence labels to indicate the reliability of each pair. 
            </p>
            <p className="chart-note">
              Start with the summary cards, including the descriptive statistics that detail the dataset used, then check the distribution charts.
              Do not rely on one average value only, but rather read median and range together, as well as pay attention to other parameters for different insights. 
            </p>
          </div>
          <div>
            <ul className="read-guide-list">
              <li><strong>Filters:</strong> Use the left panel to focus on one city, store, or category.</li>
              <li><strong>Direction:</strong> Check both positive and negative results before making claims.</li>
              <li><strong>Match check:</strong> Use size and ingredient charts to see if pairs are close matches. Match Quality refers to reviews and ratings.</li>
              <li><strong>Outliers:</strong> Review the extreme rows table to inspect unusual cases.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className="cards">
        {statCard("Rows", overall.rows)}
        {statCard("Unique Pairs", overall.pairs)}
        {statCard("Rows Shown (%)", `${filteredShare.toFixed(1)}%`, `${overall.rows}/${totalRows} rows`)}
        {statCard("Mean Pink Tax %", overall.mean.toFixed(2))}
        <article className="card card-median">
          <div className="label">Overall Median Range</div>
          <div className="value value-long">{`${overall.p40.toFixed(2)}% to ${overall.p60.toFixed(2)}%`}</div>
          <div className="help">{`Median: ${overall.median.toFixed(2)}%`}</div>
        </article>
        {statCard("Women Pay More (%)", pct(overall.posShare))}
        {statCard("Men Pay More (%)", pct(overall.negShare))}
        {statCard("No Gap (%)", pct(overall.zeroShare), "Median can be zero when many rows are exactly 0")}
      </section>

      <section className="panel descriptive-panel">
        <h2>Descriptive Statistics</h2>
        <p className="chart-note">Expanded exploratory data analysis (EDA) summary for scope, quality, comparability, and robustness under the current filters. Technical summary.</p>
        <div className="descriptive-grid">
          <article className="descriptive-card">
            <h3>Coverage and Scope</h3>
            <dl>
              <div><dt>Date window</dt><dd>{`${descriptiveStats.dateStart} to ${descriptiveStats.dateEnd}`}</dd></div>
              <div><dt>Window length</dt><dd>{descriptiveStats.daySpan === null ? "N/A" : `${descriptiveStats.daySpan} days`}</dd></div>
              <div><dt>Cities in view</dt><dd>{descriptiveStats.cityCount}</dd></div>
              <div><dt>Retailers in view</dt><dd>{descriptiveStats.retailerCount}</dd></div>
              <div><dt>Categories in view</dt><dd>{descriptiveStats.categoryCount}</dd></div>
              <div><dt>Unique brands</dt><dd>{descriptiveStats.brandCount}</dd></div>
              <div><dt>Largest retailer share</dt><dd>{`${descriptiveStats.topRetailerShare.toFixed(1)}%`}</dd></div>
              <div><dt>Largest category share</dt><dd>{`${descriptiveStats.topCategoryShare.toFixed(1)}%`}</dd></div>
            </dl>
          </article>

          <article className="descriptive-card">
            <h3>Data Quality and Labeling</h3>
            <dl>
              <div><dt>Median match quality</dt><dd>{descriptiveStats.mqMedian.toFixed(2)}</dd></div>
              <div><dt>Rows with MQ ≥ 4</dt><dd>{`${descriptiveStats.mqHighShare.toFixed(1)}%`}</dd></div>
              <div><dt>Rows with MQ ≤ 2</dt><dd>{`${descriptiveStats.mqLowShare.toFixed(1)}%`}</dd></div>
              <div><dt>High confidence labels</dt><dd>{`${descriptiveStats.confHighShare.toFixed(1)}%`}</dd></div>
              <div><dt>Medium confidence labels</dt><dd>{`${descriptiveStats.confMedShare.toFixed(1)}%`}</dd></div>
              <div><dt>Low confidence labels</dt><dd>{`${descriptiveStats.confLowShare.toFixed(1)}%`}</dd></div>
              <div><dt>Core-field completeness</dt><dd>{`${descriptiveStats.completeShare.toFixed(1)}%`}</dd></div>
              <div><dt>Rows with both PPUs</dt><dd>{`${descriptiveStats.ppuShare.toFixed(1)}%`}</dd></div>
            </dl>
          </article>

          <article className="descriptive-card">
            <h3>Comparability and Ingredients</h3>
            <dl>
              <div><dt>Ingredient coverage</dt><dd>{`${descriptiveStats.ingredientCoverage.toFixed(1)}%`}</dd></div>
              <div><dt>Median ingredient overlap</dt><dd>{`${descriptiveStats.ingredientMedian.toFixed(1)}%`}</dd></div>
              <div><dt>Ingredient overlap IQR</dt><dd>{`${descriptiveStats.ingredientIqr.toFixed(1)} pts`}</dd></div>
              <div><dt>Exact same size</dt><dd>{`${descriptiveStats.exactSizeShare.toFixed(1)}%`}</dd></div>
              <div><dt>Within ±10% size</dt><dd>{`${descriptiveStats.within10Share.toFixed(1)}%`}</dd></div>
              <div><dt>Within ±30% size</dt><dd>{`${descriptiveStats.within30Share.toFixed(1)}%`}</dd></div>
              <div><dt>Top city share</dt><dd>{`${descriptiveStats.topCityShare.toFixed(1)}%`}</dd></div>
              <div><dt>Rows in ±1% band</dt><dd>{`${descriptiveStats.nearZero1Share.toFixed(1)}%`}</dd></div>
            </dl>
          </article>

          <article className="descriptive-card">
            <h3>Robustness and Dispersion</h3>
            <dl>
              <div><dt>Standard deviation</dt><dd>{descriptiveStats.stdev.toFixed(2)}</dd></div>
              <div><dt>IQR width (Q3-Q1)</dt><dd>{descriptiveStats.iqrWidth.toFixed(2)}</dd></div>
              <div><dt>Trimmed mean (10%)</dt><dd>{descriptiveStats.trimmedMean10.toFixed(2)}</dd></div>
              <div><dt>Median absolute deviation</dt><dd>{descriptiveStats.mad.toFixed(2)}</dd></div>
              <div><dt>Rows in ±5% band</dt><dd>{`${descriptiveStats.nearZero5Share.toFixed(1)}%`}</dd></div>
              <div><dt>Extreme positive (&gt;100%)</dt><dd>{`${descriptiveStats.outlierHighShare.toFixed(1)}%`}</dd></div>
              <div><dt>Extreme negative (&lt;-50%)</dt><dd>{`${descriptiveStats.outlierLowShare.toFixed(1)}%`}</dd></div>
              <div><dt>Women/Men ratio</dt><dd>{descriptiveStats.posNegRatio.toFixed(2)}</dd></div>
            </dl>
          </article>
        </div>
      </section>

      <section className="chart-grid">
        <article className="panel chart-card">
          <h2>Pink Tax Distribution</h2>
          <p className="chart-note">Pink bars show count by pink tax range. Vertical lines mark zero, mean, and median.</p>
          <div className="chart-inline-meta">
            <span className="legend-chip chip-parity">Parity (0)</span>
            <span className="legend-chip chip-mean">{`Mean (${overall.mean.toFixed(1)}%)`}</span>
            <span className="legend-chip chip-median">{`Median (${overall.median.toFixed(1)}%)`}</span>
            <span className="legend-chip chip-positive">{`Women pay more: ${overall.posShare.toFixed(1)}%`}</span>
          </div>
          <div className="chart distribution-chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <ComposedChart data={histData} margin={{ top: 22, right: 18, left: 10, bottom: 24 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="bucketMid" tick={{ fontSize: 12 }} label={axisLabelX("Pink Tax %")} />
                <YAxis
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Count of Pairs",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle" }
                  }}
                />
                <Tooltip
                  formatter={(value) => [value, "Count"]}
                  labelFormatter={(_, payloadItems) => {
                    const item = payloadItems?.[0]?.payload;
                    return item ? `Bin: ${item.bucketLabel}` : "";
                  }}
                />
                <ReferenceLine x={0} stroke="#4b5563" strokeDasharray="4 4" />
                <ReferenceLine x={overall.mean} stroke="#be185d" strokeWidth={2.2} />
                <ReferenceLine x={overall.median} stroke="#7c3aed" strokeDasharray="2 3" strokeWidth={2.2} />
                <Bar dataKey="count" fill="#ec4899" fillOpacity={0.55} stroke="#f9a8d4" strokeWidth={1} name="Count" />
                <Line type="monotone" dataKey="count" stroke="#db2777" strokeWidth={2} dot={false} name="Trend" />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card retailer-balance-card">
          <h2>Mean Tax Direction Balance by Retailer</h2>
          <p className="chart-note">Each bar shows the percent of rows where women pay more, men pay more, or prices are equal.</p>
          <div className="chart">
            <ResponsiveContainer width="100%" height={scaled(420)}>
              <BarChart
                layout="vertical"
                data={directionByRetailer}
                barCategoryGap={12}
                margin={{ top: 34, right: 14, left: 0, bottom: 26 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  domain={[0, 100]}
                  allowDecimals={false}
                  tick={{ fontSize: 13 }}
                  tickMargin={6}
                  label={axisLabelX("Percent of Rows (%)")}
                />
                <YAxis
                  type="category"
                  dataKey="retailer"
                  width={114}
                  tick={{ fontSize: 12 }}
                  tickMargin={4}
                  tickFormatter={(v) => shortLabel(v, 16)}
                />
                <Tooltip formatter={(v) => `${Number(v).toFixed(1)}%`} />
                <Legend
                  verticalAlign="top"
                  align="center"
                  height={44}
                  wrapperStyle={{ fontSize: 12, lineHeight: 1.25 }}
                />
                <Bar dataKey="positive" stackId="a" fill="#b91c1c" name="Women pay more" />
                <Bar dataKey="negative" stackId="a" fill="#166534" name="Men pay more" />
                <Bar dataKey="zero" stackId="a" fill="#64748b" name="No difference" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card">
          <h2>Tax Variation per City</h2>
          <p className="chart-note">Top chart shows each row by city. Bottom chart shows the middle range and center value for each city.</p>
          <div className="chart stacked-charts city-box-stack">
            <ResponsiveContainer width="100%" height={scaled(230)}>
              <ScatterChart margin={{ top: 10, right: 18, left: 10, bottom: 20 }}>
                <CartesianGrid />
                <XAxis
                  type="number"
                  dataKey="x"
                  domain={[-0.5, Math.max(0.5, cityStrip.labels.length - 0.5)]}
                  ticks={cityStrip.labels.map((_, i) => i)}
                  tickFormatter={(v) => cityStrip.labels[Number(v)] || ""}
                  tick={{ fontSize: 12 }}
                />
                <YAxis
                  type="number"
                  dataKey="y"
                  tick={{ fontSize: 12 }}
                  label={axisLabelYCentered("Pink Tax %")}
                />
                <Tooltip formatter={(v) => `${Number(v).toFixed(2)}%`} />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="5 5" />
                <Scatter
                  data={cityStrip.points}
                  fill="#0f766e"
                  fillOpacity={0.36}
                  stroke="#0b3a35"
                  strokeOpacity={0.65}
                  strokeWidth={0.8}
                />
              </ScatterChart>
            </ResponsiveContainer>
            <ResponsiveContainer width="100%" height={scaled(250)}>
              <ComposedChart data={cityBoxBand} margin={{ top: 10, right: 18, left: 10, bottom: 36 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" tick={{ fontSize: 12 }} label={axisLabelXCentered("City")} />
                <YAxis domain={cityMedianDomain} tick={{ fontSize: 12 }} label={axisLabelYCentered("Pink Tax %")} />
                <Tooltip formatter={(value, name) => [`${Number(value).toFixed(2)}%`, name]} />
                <Legend verticalAlign="top" align="right" height={30} />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="iqrBase" stackId="iqr" fill="transparent" stroke="none" name="Q1 base" />
                <Bar dataKey="iqrSpan" stackId="iqr" fill="#93c5fd" name="Q1-Q3 band" />
                <Line type="linear" dataKey="p10" stroke="#64748b" strokeDasharray="3 3" name="P10 %" dot={{ r: 2 }} />
                <Line type="linear" dataKey="median" stroke="#1d3557" strokeWidth={3} name="Median %" dot={{ r: 4 }} />
                <Line type="linear" dataKey="p90" stroke="#64748b" strokeDasharray="3 3" name="P90 %" dot={{ r: 2 }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card compact-rank-card">
          <h2>Category and Brand Tax Distribution</h2>
          <p className="chart-note">Smaller ranking view next to the city box chart.</p>
          <div className="chart stacked-charts compact-rank-stack">
            <ResponsiveContainer width="100%" height={compactCategoryHeight}>
              <BarChart
                layout="vertical"
                data={compactCategoryStats}
                barCategoryGap={4}
                margin={{ top: 6, right: 6, left: 8, bottom: 24 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  tick={{ fontSize: 11 }}
                  label={{ value: "Category Mean %", position: "insideBottom", offset: -4, style: { textAnchor: "middle" } }}
                />
                <YAxis
                  type="category"
                  dataKey="key"
                  width={84}
                  tick={{ fontSize: 11 }}
                  tickMargin={6}
                  tickFormatter={(v) => shortLabel(v, 14)}
                />
                <Tooltip formatter={(value) => `${Number(value).toFixed(2)}%`} />
                <ReferenceLine x={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="mean" name="Category Mean %">
                  {compactCategoryStats.map((entry) => (
                    <Cell key={`cat-compact-cell-${entry.key}`} fill={divergingBarColor(entry.mean, DIVERGING_PALETTES.category)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            <ResponsiveContainer width="100%" height={compactBrandHeight}>
              <BarChart
                layout="vertical"
                data={compactBrandStats}
                barCategoryGap={4}
                margin={{ top: 6, right: 6, left: 8, bottom: 24 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  tick={{ fontSize: 11 }}
                  label={{ value: "Brand Mean %", position: "insideBottom", offset: -4, style: { textAnchor: "middle" } }}
                />
                <YAxis
                  type="category"
                  dataKey="key"
                  width={84}
                  tick={{ fontSize: 11 }}
                  tickMargin={6}
                  tickFormatter={(v) => shortLabel(v, 14)}
                />
                <Tooltip formatter={(value) => `${Number(value).toFixed(2)}%`} />
                <ReferenceLine x={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="mean" name="Brand Mean %">
                  {compactBrandStats.map((entry) => (
                    <Cell key={`brand-compact-cell-${entry.key}`} fill={divergingBarColor(entry.mean, DIVERGING_PALETTES.brand)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card full-width">
          <h2>Per City Category Heatmap</h2>
          <p className="chart-note">Color table of average pink tax by category and city. Red is higher for women and blue is higher for men.</p>
          <div className="heatmap-wrap">
            <table className="heatmap-table">
              <thead>
                <tr>
                  <th>Category</th>
                  {cityCategoryHeat.cities.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cityCategoryHeat.rows.map((row) => (
                  <tr key={row.category}>
                    <td>{row.category}</td>
                    {cityCategoryHeat.cities.map((c) => (
                      <td
                        key={`${row.category}-${c}`}
                        style={{ backgroundColor: heatColor(row[c], cityCategoryHeat.maxAbs) }}
                      >
                        {toNum(row[c]) === null ? "N/A" : `${Number(row[c]).toFixed(1)}%`}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className="panel chart-card full-width">
          <h2>Difference Between Tokyo and Hyderabad by Category</h2>
          <p className="chart-note">Positive bars mean Tokyo has a higher average pink tax than Hyderabad.</p>
          <div className="chart tokyo-diff-chart">
            <ResponsiveContainer width="100%" height={categoryDiffChartHeight}>
              <BarChart
                layout="vertical"
                data={displayedCategoryCityDiff}
                margin={{ top: 20, right: 20, left: 4, bottom: 12 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" tick={{ fontSize: 12 }} label={axisLabelX("Difference in Mean Pink Tax % (Tokyo - Hyderabad)")} />
                <YAxis type="category" dataKey="category" width={132} tick={{ fontSize: 12 }} tickFormatter={(v) => shortLabel(v, 18)} />
                <Tooltip formatter={(v, key, row) => [`${Number(v).toFixed(2)}%`, "Tokyo - Hyderabad"]} />
                <ReferenceLine x={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="diff" fill="#f43f5e">
                  {displayedCategoryCityDiff.map((entry) => (
                    <Cell
                      key={`diff-cell-${entry.category}`}
                      fill={divergingBarColor(entry.diff, DIVERGING_PALETTES.cityDiff)}
                    />
                  ))}
                  {showValueLabels ? (
                    <LabelList dataKey="diff" position="right" formatter={(v) => `${Number(v).toFixed(1)}%`} />
                  ) : null}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card pair-scatter-card">
          <h2>Ingredient Overlap (Between Products) vs Pink Tax</h2>
          <p className="chart-note">X axis is ingredient overlap percent. Y axis is pink tax percent.</p>
          <div className="chart pair-scatter-plot">
            <ResponsiveContainer width="100%" height={scaled(390)}>
              <ScatterChart margin={{ top: 8, right: 18, left: 10, bottom: 24 }}>
                <CartesianGrid />
                <XAxis
                  type="number"
                  dataKey="ingredient_overlap_pct"
                  name="Ingredient Overlap %"
                  tick={{ fontSize: 12 }}
                  label={axisLabelX("Ingredient Overlap %")}
                />
                <YAxis
                  type="number"
                  dataKey="pink_tax_pct"
                  name="Pink Tax %"
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Pink Tax %",
                    angle: -90,
                    position: "insideLeft",
                    dy: 14,
                    style: { textAnchor: "middle" }
                  }}
                />
                <Tooltip cursor={{ strokeDasharray: "3 3" }} formatter={(value) => Number(value).toFixed(2)} />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="5 5" />
                <Scatter
                  data={overlapScatter}
                  fill="#0f766e"
                  fillOpacity={0.34}
                  stroke="#0b3a35"
                  strokeOpacity={0.62}
                  strokeWidth={0.75}
                />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card pair-scatter-card">
          <h2>Female vs Male Unit Price (Log Scale)</h2>
          <p className="chart-note">Points above the diagonal line mean women unit price is higher.</p>
          <div className="chart pair-scatter-plot">
            <ResponsiveContainer width="100%" height={scaled(390)}>
              <ScatterChart margin={{ top: 8, right: 18, left: 10, bottom: 24 }}>
                <CartesianGrid />
                <XAxis
                  type="number"
                  dataKey="femaleLog"
                  domain={ppuLogDomain}
                  tick={{ fontSize: 12 }}
                  tickFormatter={(v) => Number(v).toFixed(1)}
                  label={axisLabelX("Female unit price (log10)")}
                />
                <YAxis
                  type="number"
                  dataKey="maleLog"
                  domain={ppuLogDomain}
                  tick={{ fontSize: 12 }}
                  tickFormatter={(v) => Number(v).toFixed(1)}
                  label={{
                    value: "Male unit price (log10)",
                    angle: -90,
                    position: "insideLeft",
                    dy: 14,
                    style: { textAnchor: "middle" }
                  }}
                />
                <Tooltip formatter={(v) => Number(v).toFixed(2)} />
                <Legend verticalAlign="top" align="right" height={34} />
                <ReferenceLine
                  segment={[
                    { x: ppuLogDomain[0], y: ppuLogDomain[0] },
                    { x: ppuLogDomain[1], y: ppuLogDomain[1] }
                  ]}
                  stroke="#475569"
                  strokeDasharray="5 5"
                />
                <Scatter
                  data={ppuScatterByCity.Hyderabad}
                  fill="#0f766e"
                  fillOpacity={0.34}
                  stroke="#0b3a35"
                  strokeOpacity={0.65}
                  strokeWidth={0.8}
                  name="Hyderabad"
                />
                <Scatter
                  data={ppuScatterByCity.Tokyo}
                  fill="#1d4ed8"
                  fillOpacity={0.34}
                  stroke="#1e3a8a"
                  strokeOpacity={0.65}
                  strokeWidth={0.8}
                  name="Tokyo"
                />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card">
          <h2>Mean Tax Direction Balance by City</h2>
          <p className="chart-note">City level percent split of women pay more, men pay more, and no gap.</p>
          <div className="chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <BarChart data={directionByCity} margin={{ top: 8, right: 18, left: 10, bottom: 24 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="city" tick={{ fontSize: 12 }} label={axisLabelX("City")} />
                <YAxis
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Percent of Rows (%)",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle", dominantBaseline: "middle" }
                  }}
                />
                <Tooltip formatter={(v) => `${Number(v).toFixed(1)}%`} />
                <Legend verticalAlign="top" align="right" height={36} />
                <Bar dataKey="positive" stackId="a" fill="#e11d48" name="Positive" />
                <Bar dataKey="negative" stackId="a" fill="#0f766e" name="Negative" />
                <Bar dataKey="zero" stackId="a" fill="#64748b" name="Zero" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card size-ratio-card">
          <h2>Size Ratio Comparability for Male vs. Female Products</h2>
          <p className="chart-note">How often women and men products have the same or similar size.</p>
          <div className="chart size-ratio-chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <BarChart data={sizeRatioData} margin={{ top: 24, right: 18, left: 20, bottom: 36 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="group"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(v) => shortLabel(v, 18)}
                  angle={-12}
                  textAnchor="end"
                  interval={0}
                  height={72}
                />
                <YAxis
                  width={72}
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Percent of Rows (%)",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle" }
                  }}
                />
                <Tooltip formatter={(v) => `${Number(v).toFixed(1)}%`} />
                <Bar dataKey="share" fill="#ec4899">
                  {showValueLabels ? (
                    <LabelList dataKey="share" position="top" formatter={(v) => `${Number(v).toFixed(1)}%`} />
                  ) : null}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card">
          <h2>Ingredient Overlap By Range Buckets</h2>
          <p className="chart-note">Average pink tax by ingredient overlap range.</p>
          <div className="chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <BarChart data={overlapBucketData} margin={{ top: 8, right: 18, left: 10, bottom: 30 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="bucket" tick={{ fontSize: 12 }} label={axisLabelX("Overlap Bucket (%)")} margin={{ top: 200 }} />
                <YAxis
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Mean Pink Tax %",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle", dominantBaseline: "middle" }
                  }}
                />
                <Tooltip formatter={(v) => `${Number(v).toFixed(2)}%`} />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="mean" fill="#be185d">
                  {showValueLabels ? (
                    <LabelList dataKey="mean" position="top" formatter={(v) => `${Number(v).toFixed(1)}%`} />
                  ) : null}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card">
          <h2>Retailer Average Price Comparison</h2>
          <p className="chart-note">This shows how much the price gap changes across retailers.</p>
          <div className="chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <ComposedChart data={retailerBoxBand} margin={{ top: 30, right: 18, left: 10, bottom: 22 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="label"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(v) => shortLabel(v, 14)}
                  angle={-18}
                  textAnchor="end"
                  interval={0}
                  height={84}
                />
                <YAxis
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Pink Tax %",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle", dominantBaseline: "middle" }
                  }}
                />
                <Tooltip formatter={(value, name) => [`${Number(value).toFixed(2)}%`, name]} />
                <Legend verticalAlign="top" align="right" height={36} />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="5 5" />
                <Bar dataKey="iqrBase" stackId="iqr" fill="transparent" stroke="none" name="Q1 base" />
                <Bar dataKey="iqrSpan" stackId="iqr" fill="#93c5fd" name="Q1-Q3 band" />
                <Line type="monotone" dataKey="p10" stroke="#64748b" strokeDasharray="3 3" name="P10 %" dot={{ r: 2 }} />
                <Line type="monotone" dataKey="median" stroke="#1d3557" strokeWidth={3} name="Median %" dot={{ r: 4 }}>
                  {showValueLabels ? (
                    <LabelList dataKey="median" position="top" formatter={(v) => `${Number(v).toFixed(1)}%`} />
                  ) : null}
                </Line>
                <Line type="monotone" dataKey="p90" stroke="#64748b" strokeDasharray="3 3" name="P90 %" dot={{ r: 2 }} />
                <Line type="monotone" dataKey="mean" stroke="#b45309" strokeWidth={2} name="Mean %" dot={{ r: 2 }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel chart-card full-width force-full-width violin-full-row">
          <h2>Top-6 Category Products Violin Tax Distribution</h2>
          <p className="chart-note">This shows the full spread of price gaps for the six biggest categories (largest sample sizes) in the filtered data.</p>
          <div className="chart">
            <LiveViolinPlot
              categories={topCategoryDistribution.categories}
              yMin={topCategoryDistribution.yMin}
              yMax={topCategoryDistribution.yMax}
              height={scaled(510)}
            />
          </div>
        </article>

        <article className="panel chart-card full-width force-full-width">
          <h2>City Median Unit Price by Gender</h2>
          <p className="chart-note">Median unit price for women and men, by city, using current filters.</p>
          <div className="chart">
            <ResponsiveContainer width="100%" height={scaled(360)}>
              <BarChart data={cityMedianPPU} margin={{ top: 8, right: 18, left: 10, bottom: 24 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="city" tick={{ fontSize: 12 }} label={axisLabelX("City")} />
                <YAxis
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Median PPU (Local)",
                    angle: -90,
                    position: "insideLeft",
                    style: { textAnchor: "middle", dominantBaseline: "middle" }
                  }}
                />
                <Tooltip formatter={(v) => (v === null ? "N/A" : Number(v).toFixed(2))} />
                <Legend verticalAlign="top" align="right" height={36} />
                <Bar dataKey="femaleMedianPPU" fill="#db2777" name="Female Median PPU" />
                <Bar dataKey="maleMedianPPU" fill="#1d4ed8" name="Male Median PPU" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>
      </section>

      <section className="panel">
        <h2>Most Extreme Filtered Rows (Top {extremeRowsCount})</h2>
        <p className="chart-note">These rows have the largest price gaps in your current filtered view; biggest outliers.</p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Pair Code</th>
                <th>City</th>
                <th>Retailer</th>
                <th>Category</th>
                <th>Brand</th>
                <th>Pink Tax %</th>
              </tr>
            </thead>
            <tbody>
              {extremes.map((r) => (
                <tr key={`${r.pair_code}-${r.retailer}-${r.pinkNum}`}>
                  <td>{r.pair_code}</td>
                  <td>{r.city}</td>
                  <td>{r.retailer}</td>
                  <td>{r.category}</td>
                  <td>{r.brand}</td>
                  <td className={r.pinkNum > 0 ? "bad" : r.pinkNum < 0 ? "good" : ""}>{r.pinkNum.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      </>
      )}
      {mainTab === "stats" && <StatisticsPanel data={regressionPayload} />}

      </div>

    </main>
  );
}
