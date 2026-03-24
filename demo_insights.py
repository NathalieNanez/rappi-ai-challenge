"""
demo_insights.py — Run the insights engine standalone and generate the HTML report.
This script works WITHOUT the full Docker stack, perfect for a quick demo.

Usage:
    ANTHROPIC_API_KEY=sk-... python demo_insights.py
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── inline minimal versions so this script is self-contained ────────────────

import pandas as pd
import numpy as np
from scipy import stats
from typing import Any
from pydantic import BaseModel, Field

WEEK_COLS_METRICS = ["L8W_ROLL","L7W_ROLL","L6W_ROLL","L5W_ROLL",
                     "L4W_ROLL","L3W_ROLL","L2W_ROLL","L1W_ROLL","L0W_ROLL"]
WEEK_LABELS = ["L8W","L7W","L6W","L5W","L4W","L3W","L2W","L1W","L0W"]
HIGHER_IS_BETTER = {
    "Lead Penetration","Perfect Orders","Gross Profit UE","Non-Pro PTC > OP",
    "Pro Adoption (Last Week Status)","% PRO Users Who Breakeven",
    "Restaurants SS > ATC CVR","Restaurants SST > SS CVR","Retail SST > SS CVR",
    "Turbo Adoption","MLTV Top Verticals Adoption",
    "% Restaurants Sessions With Optimal Assortment",
}

class Insight(BaseModel):
    """Pydantic model matching app.insights.engine.Insight."""
    category: str
    severity: str
    title: str
    description: str
    affected_zones: list[str] = Field(default_factory=list)
    metric: str = ""
    country: str = ""
    data: dict[str, Any] = Field(default_factory=dict)
    recommendation: str = ""


def load_data(path: str):
    metrics = pd.read_excel(path, sheet_name="RAW_INPUT_METRICS")
    orders  = pd.read_excel(path, sheet_name="RAW_ORDERS")
    metrics.rename(columns=dict(zip(WEEK_COLS_METRICS, WEEK_LABELS)), inplace=True)
    return metrics, orders


def detect_anomalies(metrics, limit=8):
    results = []
    for metric in metrics["METRIC"].unique():
        mdf = metrics[metrics["METRIC"] == metric].copy()
        mdf = mdf[(mdf["L1W"].notna()) & (mdf["L0W"].notna()) & (mdf["L1W"] != 0)]
        mdf["pct_change"] = (mdf["L0W"] - mdf["L1W"]) / mdf["L1W"].abs()
        positive_good = metric in HIGHER_IS_BETTER
        bad = mdf[mdf["pct_change"] < -0.10] if positive_good else mdf[mdf["pct_change"] > 0.10]
        for _, row in bad.nsmallest(2, "pct_change" if positive_good else "pct_change").iterrows():
            pct = row["pct_change"] * 100
            results.append(Insight(
                category="anomaly",
                severity="critical" if abs(pct) > 20 else "warning",
                title=f"Anomalía: {metric} en {row['ZONE']} ({row['COUNTRY']})",
                description=(
                    f"{metric} cambió {pct:.1f}% de L1W ({row['L1W']:.3f}) "
                    f"a L0W ({row['L0W']:.3f}) en {row['ZONE']}, {row['CITY']} ({row['COUNTRY']}). "
                    f"Zona tipo: {row['ZONE_TYPE']} — {row['ZONE_PRIORITIZATION']}."
                ),
                affected_zones=[row["ZONE"]], metric=metric, country=row["COUNTRY"],
                data={"pct_change": round(pct,2), "L1W": round(row["L1W"],4),
                      "L0W": round(row["L0W"],4), "prioritization": row["ZONE_PRIORITIZATION"]},
                recommendation=(
                    f"Investigar causa raíz en {row['ZONE']}. "
                    f"Comparar con zonas {row['ZONE_TYPE']} en {row['COUNTRY']}."
                ),
            ))
    results.sort(key=lambda i: -abs(i.data.get("pct_change", 0)))
    return results[:limit]


def count_consecutive(values, higher_is_better):
    c = 0
    for i in range(len(values)-1, 0, -1):
        if higher_is_better:
            if values[i] < values[i-1]: c += 1
            else: break
        else:
            if values[i] > values[i-1]: c += 1
            else: break
    return c


def detect_trends(metrics, limit=8):
    results = []
    for metric in metrics["METRIC"].unique():
        pos_good = metric in HIGHER_IS_BETTER
        mdf = metrics[metrics["METRIC"] == metric].dropna(subset=WEEK_LABELS)
        for _, row in mdf.iterrows():
            vals = [row[w] for w in WEEK_LABELS]
            consec = count_consecutive(vals, pos_good)
            if consec >= 3:
                total_chg = (vals[-1] - vals[-consec]) / abs(vals[-consec]) * 100 if vals[-consec] != 0 else 0
                results.append(Insight(
                    category="trend",
                    severity="critical" if (abs(total_chg) > 15 or row["ZONE_PRIORITIZATION"] == "High Priority") else "warning",
                    title=f"Deterioro sostenido: {metric} en {row['ZONE']} ({row['COUNTRY']})",
                    description=(
                        f"{metric} ha empeorado {consec} semanas consecutivas en "
                        f"{row['ZONE']} ({row['CITY']}, {row['COUNTRY']}). "
                        f"Cambio acumulado: {total_chg:.1f}%. Valor actual: {vals[-1]:.3f}."
                    ),
                    affected_zones=[row["ZONE"]], metric=metric, country=row["COUNTRY"],
                    data={"consecutive_weeks": consec, "total_change_pct": round(total_chg,2),
                          "current_value": round(vals[-1],4)},
                    recommendation=(
                        f"Problema estructural en {row['ZONE']}: {consec} semanas de deterioro. "
                        f"Revisar cambios en competidores, cobertura y calidad de partners."
                    ),
                ))
    results.sort(key=lambda i: (-i.data.get("consecutive_weeks",0), -abs(i.data.get("total_change_pct",0))))
    return results[:limit]


def detect_benchmarks(metrics, limit=8):
    results = []
    for metric in ["Perfect Orders", "Lead Penetration", "Gross Profit UE"]:
        mdf = metrics[metrics["METRIC"] == metric].dropna(subset=["L0W"])
        for (country, ztype), grp in mdf.groupby(["COUNTRY", "ZONE_TYPE"]):
            if len(grp) < 4: continue
            mean, std = grp["L0W"].mean(), grp["L0W"].std()
            if std == 0 or np.isnan(std): continue
            grp = grp.copy()
            grp["z"] = (grp["L0W"] - mean) / std
            for _, row in grp[grp["z"] < -1.5].nsmallest(2, "L0W").iterrows():
                gap = (row["L0W"] - mean) / abs(mean) * 100
                results.append(Insight(
                    category="benchmark",
                    severity="warning",
                    title=f"Bajo vs peers: {metric} en {row['ZONE']} ({country})",
                    description=(
                        f"{row['ZONE']} tiene {metric}={row['L0W']:.3f} vs promedio "
                        f"de zonas {ztype} en {country}: {mean:.3f} ({abs(gap):.1f}% abajo). "
                        f"Z-score: {row['z']:.2f}."
                    ),
                    affected_zones=[row["ZONE"]], metric=metric, country=country,
                    data={"value": round(row["L0W"],4), "peer_mean": round(mean,4),
                          "gap_pct": round(gap,2), "z_score": round(row["z"],2)},
                    recommendation=(
                        f"Investigar por qué {row['ZONE']} tiene {metric} tan bajo vs "
                        f"sus peers {ztype} en {country}."
                    ),
                ))
    return results[:limit]


def detect_correlations(metrics, limit=5):
    results = []
    pairs = [
        ("Lead Penetration","Perfect Orders"),
        ("Non-Pro PTC > OP","Gross Profit UE"),
        ("Pro Adoption (Last Week Status)","MLTV Top Verticals Adoption"),
        ("Restaurants SS > ATC CVR","Gross Profit UE"),
    ]
    for country, grp in metrics.groupby("COUNTRY"):
        for m1, m2 in pairs:
            d1 = grp[grp["METRIC"]==m1][["ZONE","L0W"]].rename(columns={"L0W":m1})
            d2 = grp[grp["METRIC"]==m2][["ZONE","L0W"]].rename(columns={"L0W":m2})
            merged = d1.merge(d2, on="ZONE").dropna()
            if len(merged) < 8: continue
            r, p = stats.pearsonr(merged[m1], merged[m2])
            if abs(r) > 0.55 and p < 0.05:
                strength = "fuerte" if abs(r) > 0.75 else "moderada"
                direction = "positiva" if r > 0 else "negativa"
                results.append(Insight(
                    category="correlation", severity="info",
                    title=f"Correlación {strength}: {m1} ↔ {m2} en {country}",
                    description=f"r={r:.2f}, p={p:.3f} en {len(merged)} zonas de {country}.",
                    metric=f"{m1}|{m2}", country=country,
                    data={"correlation": round(r,3), "p_value": round(p,4), "n": len(merged)},
                    recommendation=f"Intervenciones en {m1} tienen efecto multiplicador en {m2} en {country}.",
                ))
    results.sort(key=lambda i: -abs(i.data.get("correlation",0)))
    return results[:limit]


def detect_opportunities(metrics, orders, limit=5):
    orders = orders[(orders["L0W"] > 0) & (orders["L8W"] > 0)].copy()
    orders["growth"] = (orders["L0W"] - orders["L8W"]) / orders["L8W"]
    lead = metrics[metrics["METRIC"]=="Lead Penetration"][["ZONE","L0W"]].rename(columns={"L0W":"lp"})
    results = []
    for _, row in orders.nlargest(30,"growth").iterrows():
        zl = lead[lead["ZONE"]==row["ZONE"]]
        if zl.empty: continue
        lp = zl.iloc[0]["lp"]
        if lp < 0.60:
            results.append(Insight(
                category="opportunity", severity="info",
                title=f"Oportunidad: {row['ZONE']} ({row['COUNTRY']})",
                description=(
                    f"{row['ZONE']} creció +{row['growth']*100:.1f}% en órdenes (L8W→L0W) "
                    f"pero Lead Penetration es {lp:.1%} — alta demanda, baja oferta."
                ),
                affected_zones=[row["ZONE"]], metric="Orders+Lead Penetration", country=row["COUNTRY"],
                data={"orders_growth_pct": round(row["growth"]*100,2),
                      "lead_penetration": round(lp,4), "orders_L0W": int(row["L0W"])},
                recommendation=f"Priorizar captación de tiendas en {row['ZONE']} — demanda existe.",
            ))
    return results[:limit]


def generate_html_report(insights):
    """Generate standalone HTML report."""
    SEVERITY_COLOR = {"critical":"#dc2626","warning":"#d97706","info":"#2563eb"}
    CAT_ICON = {"anomaly":"🚨","trend":"📉","benchmark":"📊","correlation":"🔗","opportunity":"🚀"}

    critical_n   = sum(1 for i in insights if i.severity == "critical")
    warning_n    = sum(1 for i in insights if i.severity == "warning")
    opps_n       = sum(1 for i in insights if i.category == "opportunity")
    zones_n      = len({z for i in insights for z in i.affected_zones})
    countries_n  = len({i.country for i in insights if i.country})

    cards_html = ""
    for ins in insights:
        chips = "".join(
            f'<span style="font-size:11px;background:#f3f4f6;padding:2px 8px;border-radius:4px;margin:2px;font-family:monospace">'
            f'{k}: {v}</span>'
            for k, v in ins.data.items()
        )
        badge_bg = {"critical":"#fee2e2","warning":"#fef3c7","info":"#dbeafe"}.get(ins.severity,"#f3f4f6")
        badge_fg = SEVERITY_COLOR.get(ins.severity,"#374151")
        cards_html += f"""
        <div class="card" data-cat="{ins.category}">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
            <span style="background:{badge_bg};color:{badge_fg};font-size:11px;font-weight:700;
                         padding:3px 10px;border-radius:20px">{ins.severity.upper()}</span>
            <strong style="font-size:15px">{CAT_ICON.get(ins.category,"📌")} {ins.title}</strong>
          </div>
          <p style="font-size:14px;color:#374151;margin:8px 0">{ins.description}</p>
          <div style="margin:6px 0">{chips}</div>
          <div style="background:#f0fdf4;border-left:3px solid #16a34a;border-radius:0 8px 8px 0;
                      padding:10px 14px;margin-top:10px;font-size:13px;color:#166534">
            💡 <strong>Recomendación:</strong> {ins.recommendation}
          </div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reporte Ejecutivo — Rappi Analytics</title>
<style>
  body{{font-family:Inter,system-ui,sans-serif;background:#f9fafb;color:#111827;line-height:1.6;margin:0}}
  .page{{max-width:960px;margin:0 auto;padding:40px 24px}}
  header{{border-bottom:3px solid #ff441f;padding-bottom:24px;margin-bottom:32px}}
  h1{{font-size:28px;font-weight:700;color:#ff441f;margin:0}}
  .kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:16px;margin-bottom:32px}}
  .kpi{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:20px;text-align:center}}
  .kpi .num{{font-size:36px;font-weight:700}}
  .kpi .lbl{{font-size:12px;color:#6b7280;margin-top:4px}}
  .filters{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}}
  .filter-btn{{font-size:13px;padding:6px 14px;border-radius:20px;border:1px solid #e5e7eb;
               background:white;cursor:pointer;transition:all .15s}}
  .filter-btn.active,.filter-btn:hover{{background:#ff441f;color:white;border-color:#ff441f}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:20px;margin-bottom:12px}}
  footer{{text-align:center;color:#9ca3af;font-size:12px;padding-top:32px;
          border-top:1px solid #e5e7eb;margin-top:40px}}
  h2{{font-size:20px;font-weight:600;border-left:4px solid #ff441f;padding-left:12px;margin:0 0 20px}}
</style>
</head>
<body>
<div class="page">
<header>
  <h1>🚦 Reporte Ejecutivo — Rappi Analytics</h1>
  <p style="color:#6b7280;margin:4px 0 0;font-size:14px">
    Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')} · {len(insights)} hallazgos · 9 semanas de datos · 9 países
  </p>
</header>

<div class="kpi-row">
  <div class="kpi"><div class="num" style="color:#dc2626">{critical_n}</div><div class="lbl">Alertas críticas</div></div>
  <div class="kpi"><div class="num" style="color:#d97706">{warning_n}</div><div class="lbl">Advertencias</div></div>
  <div class="kpi"><div class="num" style="color:#2563eb">{opps_n}</div><div class="lbl">Oportunidades</div></div>
  <div class="kpi"><div class="num" style="color:#16a34a">{zones_n}</div><div class="lbl">Zonas afectadas</div></div>
  <div class="kpi"><div class="num" style="color:#7c3aed">{countries_n}</div><div class="lbl">Países</div></div>
</div>

<h2>Hallazgos detectados</h2>
<div class="filters">
  <button class="filter-btn active" onclick="filter('all')">Todos ({len(insights)})</button>
  <button class="filter-btn" onclick="filter('anomaly')">🚨 Anomalías</button>
  <button class="filter-btn" onclick="filter('trend')">📉 Tendencias</button>
  <button class="filter-btn" onclick="filter('benchmark')">📊 Benchmark</button>
  <button class="filter-btn" onclick="filter('correlation')">🔗 Correlaciones</button>
  <button class="filter-btn" onclick="filter('opportunity')">🚀 Oportunidades</button>
</div>

<div id="cards">{cards_html}</div>

<footer>Rappi Analytics · Sistema de Análisis Inteligente</footer>
</div>
<script>
function filter(cat){{
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  event.target.classList.add('active');
  document.querySelectorAll('.card').forEach(c=>{{
    c.style.display=(cat==='all'||c.dataset.cat===cat)?'':'none';
  }});
}}
</script>
</body></html>"""


if __name__ == "__main__":
    DATA_PATH = os.getenv("DATA_PATH", "data/raw/rappi_data.xlsx")

    if not Path(DATA_PATH).exists():
        print(f"ERROR: Data file not found at {DATA_PATH}")
        sys.exit(1)

    print("📊 Loading data...")
    metrics, orders = load_data(DATA_PATH)
    print(f"   {len(metrics):,} metric rows, {len(orders):,} order rows loaded")

    print("\n🔍 Running insight detectors...")
    all_insights = []
    all_insights.extend(detect_anomalies(metrics, limit=8))
    print(f"   Anomalies:    {sum(1 for i in all_insights if i.category=='anomaly')}")
    all_insights.extend(detect_trends(metrics, limit=8))
    print(f"   Trends:       {sum(1 for i in all_insights if i.category=='trend')}")
    all_insights.extend(detect_benchmarks(metrics, limit=8))
    print(f"   Benchmarks:   {sum(1 for i in all_insights if i.category=='benchmark')}")
    all_insights.extend(detect_correlations(metrics, limit=5))
    print(f"   Correlations: {sum(1 for i in all_insights if i.category=='correlation')}")
    all_insights.extend(detect_opportunities(metrics, orders, limit=5))
    print(f"   Opportunities:{sum(1 for i in all_insights if i.category=='opportunity')}")

    # Sort by severity
    sev_order = {"critical":0,"warning":1,"info":2}
    all_insights.sort(key=lambda i: (sev_order.get(i.severity,9),))

    print(f"\n📋 Total insights: {len(all_insights)}")
    print(f"   Critical: {sum(1 for i in all_insights if i.severity=='critical')}")
    print(f"   Warning:  {sum(1 for i in all_insights if i.severity=='warning')}")
    print(f"   Info:     {sum(1 for i in all_insights if i.severity=='info')}")

    print("\n📄 Generating HTML report...")
    Path("reports").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = Path(f"reports/rappi_insights_{timestamp}.html")
    html = generate_html_report(all_insights)
    output.write_text(html, encoding="utf-8")
    print(f"   ✅ Report saved: {output}")
    print(f"\n🎉 Done! Open {output} in your browser.")
