"""
Insights Engine: automatically analyses the Rappi dataset and produces
structured findings across 5 categories:
  1. Anomalies      -- week-over-week spikes / drops > threshold
  2. Trends         -- 3+ consecutive weeks of deterioration
  3. Benchmarking   -- same-country Wealthy vs Non-Wealthy comparison
  4. Correlations   -- metric pairs that co-move
  5. Opportunities  -- zones with growth potential
"""

import logging
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from scipy import stats

from app.data.loader import get_loader

logger = logging.getLogger(__name__)

WEEK_COLS = ["L8W", "L7W", "L6W", "L5W", "L4W", "L3W", "L2W", "L1W", "L0W"]

# Metrics where HIGHER is better (for trend direction logic).
HIGHER_IS_BETTER = {
    "Lead Penetration", "Perfect Orders", "Gross Profit UE",
    "Non-Pro PTC > OP", "Pro Adoption (Last Week Status)",
    "% PRO Users Who Breakeven", "Restaurants SS > ATC CVR",
    "Restaurants SST > SS CVR", "Retail SST > SS CVR",
    "Turbo Adoption", "MLTV Top Verticals Adoption",
    "% Restaurants Sessions With Optimal Assortment",
}
LOWER_IS_BETTER = {"Restaurants Markdowns / GMV"}

ANOMALY_THRESHOLD = 0.10   # 10% change week-over-week = anomaly
TREND_WEEKS      = 3       # minimum consecutive weeks for a trend


class Insight(BaseModel):
    """Structured insight detected by the engine. Uses pydantic for validation."""

    category: str = Field(description="anomaly | trend | benchmark | correlation | opportunity")
    severity: str = Field(description="critical | warning | info")
    title: str
    description: str
    affected_zones: list[str] = Field(default_factory=list)
    metric: str = ""
    country: str = ""
    data: dict[str, Any] = Field(default_factory=dict)
    recommendation: str = ""


class InsightEngine:
    """Runs all insight detectors and returns a ranked list of Insight objects."""

    def __init__(self):
        self.loader = get_loader()
        self._metrics_wide: pd.DataFrame | None = None
        self._orders_wide: pd.DataFrame | None = None

    def run(self, max_insights_per_category: int = 5) -> list[Insight]:
        """Entry point: run all detectors and return insights sorted by severity."""
        self._metrics_wide = self.loader.query(
            "SELECT * FROM metrics_wide"
        )
        self._orders_wide = self.loader.query(
            "SELECT * FROM orders_wide"
        )

        insights: list[Insight] = []

        detectors = [
            self._detect_anomalies,
            self._detect_deteriorating_trends,
            self._detect_improving_trends,
            self._detect_benchmarking,
            self._detect_correlations,
            self._detect_opportunities,
        ]

        for detector in detectors:
            try:
                found = detector(max_insights_per_category)
                insights.extend(found)
                logger.info(f"{detector.__name__}: {len(found)} insights")
            except Exception as e:
                logger.error(f"{detector.__name__} failed: {e}", exc_info=True)

        return self._rank_insights(insights)

    # ------------------------------------------------------------------
    # 1. Anomaly Detection
    # ------------------------------------------------------------------

    def _detect_anomalies(self, limit: int) -> list[Insight]:
        """Zones where last week's change vs prior week > ANOMALY_THRESHOLD."""
        df = self._metrics_wide.copy()
        results: list[Insight] = []

        for metric in df["METRIC"].unique():
            mdf = df[df["METRIC"] == metric].copy()
            if len(mdf) < 3:
                continue

            # Calculate % change L1W → L0W
            mdf = mdf[(mdf["L1W"].notna()) & (mdf["L0W"].notna()) & (mdf["L1W"] != 0)]
            mdf["pct_change"] = (mdf["L0W"] - mdf["L1W"]) / mdf["L1W"].abs()

            positive_is_good = metric in HIGHER_IS_BETTER

            # Deteriorations
            if positive_is_good:
                bad = mdf[mdf["pct_change"] < -ANOMALY_THRESHOLD].copy()
            else:
                bad = mdf[mdf["pct_change"] > ANOMALY_THRESHOLD].copy()

            bad = bad.nlargest(3, "pct_change" if not positive_is_good else "pct_change",
                               keep="all").head(3)

            for _, row in bad.iterrows():
                pct = row["pct_change"] * 100
                severity = "critical" if abs(pct) > 20 else "warning"
                direction = "cayó" if pct < 0 else "subió"
                results.append(Insight(
                    category="anomaly",
                    severity=severity,
                    title=f"Anomalía en {metric} — {row['ZONE']} ({row['COUNTRY']})",
                    description=(
                        f"{metric} {direction} {abs(pct):.1f}% de L1W ({row['L1W']:.3f}) "
                        f"a L0W ({row['L0W']:.3f}) en la zona {row['ZONE']}, "
                        f"{row['CITY']} ({row['COUNTRY']}). "
                        f"Tipo de zona: {row['ZONE_TYPE']} — {row['ZONE_PRIORITIZATION']}."
                    ),
                    affected_zones=[row["ZONE"]],
                    metric=metric,
                    country=row["COUNTRY"],
                    data={
                        "pct_change": round(pct, 2),
                        "L1W": round(row["L1W"], 4),
                        "L0W": round(row["L0W"], 4),
                        "zone_type": row["ZONE_TYPE"],
                        "prioritization": row["ZONE_PRIORITIZATION"],
                    },
                    recommendation=(
                        f"Investigar causa raíz en {row['ZONE']} inmediatamente. "
                        f"Comparar con zonas similares ({row['ZONE_TYPE']}) en {row['COUNTRY']}. "
                        f"Si es High Priority, escalar al equipo de Operations."
                    ),
                ))

        results.sort(key=lambda i: abs(i.data.get("pct_change", 0)), reverse=True)
        return results[:limit]

    # ------------------------------------------------------------------
    # 2. Deteriorating Trends (3+ consecutive weeks)
    # ------------------------------------------------------------------

    def _detect_deteriorating_trends(self, limit: int) -> list[Insight]:
        df = self._metrics_wide.copy()
        results: list[Insight] = []

        for metric in df["METRIC"].unique():
            positive_is_good = metric in HIGHER_IS_BETTER
            mdf = df[df["METRIC"] == metric][["COUNTRY","CITY","ZONE","ZONE_TYPE",
                                              "ZONE_PRIORITIZATION"] + WEEK_COLS].dropna()

            for _, row in mdf.iterrows():
                values = [row[w] for w in WEEK_COLS]
                consecutive = self._count_consecutive_deterioration(values, positive_is_good)

                if consecutive >= TREND_WEEKS:
                    total_change = (values[-1] - values[-consecutive]) / abs(values[-consecutive]) * 100 \
                        if values[-consecutive] != 0 else 0
                    severity = "critical" if (abs(total_change) > 15 or
                               row["ZONE_PRIORITIZATION"] == "High Priority") else "warning"
                    results.append(Insight(
                        category="trend",
                        severity=severity,
                        title=f"Deterioro sostenido: {metric} en {row['ZONE']} ({row['COUNTRY']})",
                        description=(
                            f"{metric} ha empeorado durante {consecutive} semanas consecutivas "
                            f"en {row['ZONE']} ({row['CITY']}, {row['COUNTRY']}). "
                            f"Cambio acumulado: {total_change:.1f}%. "
                            f"Valor actual: {values[-1]:.3f}."
                        ),
                        affected_zones=[row["ZONE"]],
                        metric=metric,
                        country=row["COUNTRY"],
                        data={
                            "consecutive_weeks": consecutive,
                            "total_change_pct": round(total_change, 2),
                            "current_value": round(values[-1], 4),
                            "values": [round(v, 4) for v in values[-consecutive:]],
                            "prioritization": row["ZONE_PRIORITIZATION"],
                        },
                        recommendation=(
                            f"Revisar operaciones en {row['ZONE']} — tendencia de {consecutive} semanas "
                            f"indica problema estructural, no puntual. "
                            f"Analizar: cambios en competidores, cobertura de tiendas, "
                            f"incidencias de calidad en el período."
                        ),
                    ))

        results.sort(key=lambda i: (
            -i.data.get("consecutive_weeks", 0),
            -abs(i.data.get("total_change_pct", 0))
        ))
        return results[:limit]

    # ------------------------------------------------------------------
    # 3. Improving Trends (opportunities)
    # ------------------------------------------------------------------

    def _detect_improving_trends(self, limit: int) -> list[Insight]:
        df = self._metrics_wide.copy()
        results: list[Insight] = []

        for metric in ["Lead Penetration", "Perfect Orders", "Gross Profit UE", "Orders"]:
            mdf = df[df["METRIC"] == metric][["COUNTRY","CITY","ZONE","ZONE_TYPE",
                                              "ZONE_PRIORITIZATION"] + WEEK_COLS].dropna()
            for _, row in mdf.iterrows():
                values = [row[w] for w in WEEK_COLS]
                consecutive = self._count_consecutive_deterioration(values, higher_is_better=False)
                if consecutive >= TREND_WEEKS:
                    total_change = (values[-1] - values[-consecutive]) / abs(values[-consecutive]) * 100 \
                        if values[-consecutive] != 0 else 0
                    if total_change > 8:
                        results.append(Insight(
                            category="opportunity",
                            severity="info",
                            title=f"Crecimiento sostenido: {metric} en {row['ZONE']} ({row['COUNTRY']})",
                            description=(
                                f"{metric} ha mejorado {consecutive} semanas consecutivas "
                                f"en {row['ZONE']} ({row['COUNTRY']}). "
                                f"Crecimiento acumulado: +{total_change:.1f}%."
                            ),
                            affected_zones=[row["ZONE"]],
                            metric=metric,
                            country=row["COUNTRY"],
                            data={"consecutive_weeks": consecutive, "total_change_pct": round(total_change, 2)},
                            recommendation=(
                                f"Identificar qué está funcionando en {row['ZONE']} y replicar "
                                f"en zonas similares ({row['ZONE_TYPE']}) del mismo país."
                            ),
                        ))

        results.sort(key=lambda i: -i.data.get("total_change_pct", 0))
        return results[:limit]

    # ------------------------------------------------------------------
    # 4. Benchmarking — zones diverging from same-country, same-type peers
    # ------------------------------------------------------------------

    def _detect_benchmarking(self, limit: int) -> list[Insight]:
        df = self._metrics_wide.copy()
        results: list[Insight] = []

        key_metrics = ["Perfect Orders", "Lead Penetration", "Gross Profit UE"]

        for metric in key_metrics:
            mdf = df[df["METRIC"] == metric].dropna(subset=["L0W"])

            for (country, zone_type), group in mdf.groupby(["COUNTRY", "ZONE_TYPE"]):
                if len(group) < 4:
                    continue

                mean_val = group["L0W"].mean()
                std_val  = group["L0W"].std()
                if std_val == 0 or np.isnan(std_val):
                    continue

                # Z-score based outliers
                group = group.copy()
                group["z_score"] = (group["L0W"] - mean_val) / std_val

                underperformers = group[group["z_score"] < -1.5].nsmallest(2, "L0W")
                overperformers  = group[group["z_score"] >  1.5].nlargest(2, "L0W")

                for _, row in underperformers.iterrows():
                    gap_pct = (row["L0W"] - mean_val) / abs(mean_val) * 100
                    results.append(Insight(
                        category="benchmark",
                        severity="warning",
                        title=f"Bajo vs peers: {metric} en {row['ZONE']} ({country})",
                        description=(
                            f"{row['ZONE']} tiene {metric} de {row['L0W']:.3f}, "
                            f"vs promedio de zonas {zone_type} en {country}: {mean_val:.3f} "
                            f"({abs(gap_pct):.1f}% por debajo). "
                            f"Z-score: {row['z_score']:.2f}."
                        ),
                        affected_zones=[row["ZONE"]],
                        metric=metric,
                        country=country,
                        data={
                            "zone_value": round(row["L0W"], 4),
                            "peer_mean": round(mean_val, 4),
                            "gap_pct": round(gap_pct, 2),
                            "z_score": round(row["z_score"], 2),
                            "zone_type": zone_type,
                            "peer_count": len(group),
                        },
                        recommendation=(
                            f"Investigar por qué {row['ZONE']} tiene {metric} tan bajo vs "
                            f"sus peers ({zone_type} en {country}). "
                            f"Revisar: oferta de tiendas, calidad de partners, cobertura de turbo."
                        ),
                    ))

        results.sort(key=lambda i: i.data.get("z_score", 0))
        return results[:limit]

    # ------------------------------------------------------------------
    # 5. Correlations — metric pairs that co-move at country level
    # ------------------------------------------------------------------

    def _detect_correlations(self, limit: int) -> list[Insight]:
        df = self._metrics_wide.copy()
        results: list[Insight] = []

        target_pairs = [
            ("Lead Penetration", "Perfect Orders"),
            ("Lead Penetration", "Gross Profit UE"),
            ("Non-Pro PTC > OP", "Gross Profit UE"),
            ("Pro Adoption (Last Week Status)", "MLTV Top Verticals Adoption"),
            ("Restaurants SS > ATC CVR", "Gross Profit UE"),
        ]

        for country, group in df.groupby("COUNTRY"):
            for m1, m2 in target_pairs:
                df1 = group[group["METRIC"] == m1][["ZONE", "L0W"]].rename(columns={"L0W": m1})
                df2 = group[group["METRIC"] == m2][["ZONE", "L0W"]].rename(columns={"L0W": m2})
                merged = df1.merge(df2, on="ZONE").dropna()
                if len(merged) < 8:
                    continue

                corr, pval = stats.pearsonr(merged[m1], merged[m2])
                if abs(corr) > 0.55 and pval < 0.05:
                    direction = "positiva" if corr > 0 else "negativa"
                    strength = "fuerte" if abs(corr) > 0.75 else "moderada"
                    results.append(Insight(
                        category="correlation",
                        severity="info",
                        title=f"Correlación {strength}: {m1} ↔ {m2} en {country}",
                        description=(
                            f"En {country}, existe una correlación {direction} {strength} "
                            f"(r={corr:.2f}, p={pval:.3f}) entre {m1} y {m2} "
                            f"across {len(merged)} zonas."
                        ),
                        metric=f"{m1} | {m2}",
                        country=country,
                        data={
                            "correlation": round(corr, 3),
                            "p_value": round(pval, 4),
                            "n_zones": len(merged),
                            "metric_1": m1,
                            "metric_2": m2,
                        },
                        recommendation=(
                            f"Zonas con bajo {m1} en {country} probablemente también tienen "
                            f"bajo {m2}. Intervenciones en {m1} pueden tener efecto multiplicador."
                        ),
                    ))

        results.sort(key=lambda i: -abs(i.data.get("correlation", 0)))
        return results[:limit]

    # ------------------------------------------------------------------
    # 6. Opportunities — strong orders growth + room in metrics
    # ------------------------------------------------------------------

    def _detect_opportunities(self, limit: int) -> list[Insight]:
        orders = self._orders_wide.copy()
        metrics = self._metrics_wide.copy()
        results: list[Insight] = []

        orders = orders[(orders["L0W"] > 0) & (orders["L8W"] > 0)].copy()
        orders["orders_growth"] = (orders["L0W"] - orders["L8W"]) / orders["L8W"]

        top_growing = orders.nlargest(20, "orders_growth")

        lead_df = metrics[metrics["METRIC"] == "Lead Penetration"][
            ["COUNTRY","CITY","ZONE","L0W"]
        ].rename(columns={"L0W": "lead_pen"})

        for _, row in top_growing.iterrows():
            zone_lead = lead_df[lead_df["ZONE"] == row["ZONE"]]
            if zone_lead.empty:
                continue
            lead_val = zone_lead.iloc[0]["lead_pen"]
            growth_pct = row["orders_growth"] * 100

            if lead_val < 0.60:  # growing zone with low store coverage
                results.append(Insight(
                    category="opportunity",
                    severity="info",
                    title=f"Oportunidad de expansión: {row['ZONE']} ({row['COUNTRY']})",
                    description=(
                        f"{row['ZONE']} ({row['CITY']}, {row['COUNTRY']}) creció "
                        f"+{growth_pct:.1f}% en órdenes (L8W→L0W) pero tiene Lead Penetration "
                        f"de solo {lead_val:.1%} — baja cobertura de tiendas en zona de alto crecimiento."
                    ),
                    affected_zones=[row["ZONE"]],
                    metric="Orders + Lead Penetration",
                    country=row["COUNTRY"],
                    data={
                        "orders_growth_pct": round(growth_pct, 2),
                        "orders_l0w": int(row["L0W"]),
                        "lead_penetration": round(lead_val, 4),
                    },
                    recommendation=(
                        f"Priorizar captación de nuevas tiendas en {row['ZONE']}. "
                        f"La demanda existe (crecimiento de {growth_pct:.1f}% en órdenes) "
                        f"pero la oferta está limitada (Lead Penetration {lead_val:.1%})."
                    ),
                ))

        return results[:limit]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _count_consecutive_deterioration(values: list[float], higher_is_better: bool) -> int:
        """Count trailing consecutive weeks of deterioration."""
        consecutive = 0
        for i in range(len(values) - 1, 0, -1):
            if higher_is_better:
                if values[i] < values[i - 1]:
                    consecutive += 1
                else:
                    break
            else:
                if values[i] > values[i - 1]:
                    consecutive += 1
                else:
                    break
        return consecutive

    @staticmethod
    def _rank_insights(insights: list[Insight]) -> list[Insight]:
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        category_order = {"anomaly": 0, "trend": 1, "benchmark": 2, "correlation": 3, "opportunity": 4}
        return sorted(
            insights,
            key=lambda i: (severity_order.get(i.severity, 9), category_order.get(i.category, 9))
        )
