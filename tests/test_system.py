"""
tests/test_system.py — Full test suite for the Rappi AI Analytics System.

Run with:
    python -m pytest tests/ -v
    python tests/test_system.py          # standalone (no pytest needed)
"""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make the project root importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

DATA_PATH = os.getenv("DATA_PATH", "data/raw/rappi_data.xlsx")
DATA_AVAILABLE = Path(DATA_PATH).exists()

WEEK_OLD = ["L8W_ROLL","L7W_ROLL","L6W_ROLL","L5W_ROLL","L4W_ROLL","L3W_ROLL","L2W_ROLL","L1W_ROLL","L0W_ROLL"]
WEEK_NEW = ["L8W","L7W","L6W","L5W","L4W","L3W","L2W","L1W","L0W"]

def load_test_data():
    metrics = pd.read_excel(DATA_PATH, sheet_name="RAW_INPUT_METRICS")
    orders  = pd.read_excel(DATA_PATH, sheet_name="RAW_ORDERS")
    metrics.rename(columns=dict(zip(WEEK_OLD, WEEK_NEW)), inplace=True)
    return metrics, orders


# ──────────────────────────────────────────────────────────────────────────────
# 1. Data Layer Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestDataLoader(unittest.TestCase):
    """Tests for app/data/loader.py"""

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_metrics_shape(self):
        metrics, _ = load_test_data()
        self.assertGreater(len(metrics), 10000, "Expected 12k+ metric rows")
        self.assertIn("METRIC", metrics.columns)
        self.assertIn("COUNTRY", metrics.columns)
        self.assertIn("L0W", metrics.columns)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_orders_shape(self):
        _, orders = load_test_data()
        self.assertGreater(len(orders), 1000)
        self.assertIn("L0W", orders.columns)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_countries(self):
        metrics, _ = load_test_data()
        countries = set(metrics["COUNTRY"].unique())
        expected = {"AR", "BR", "CL", "CO", "CR", "EC", "MX", "PE", "UY"}
        self.assertEqual(countries, expected)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_all_13_metrics_present(self):
        metrics, _ = load_test_data()
        found = set(metrics["METRIC"].unique())
        self.assertEqual(len(found), 13, f"Expected 13 metrics, found {len(found)}: {found}")

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_week_columns_renamed(self):
        metrics, _ = load_test_data()
        for col in WEEK_NEW:
            self.assertIn(col, metrics.columns, f"Missing renamed column: {col}")
        for old in WEEK_OLD:
            self.assertNotIn(old, metrics.columns, f"Old column still present: {old}")

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_zone_type_values(self):
        metrics, _ = load_test_data()
        zone_types = set(metrics["ZONE_TYPE"].dropna().unique())
        self.assertLessEqual(zone_types, {"Wealthy", "Non Wealthy"})

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_prioritization_values(self):
        metrics, _ = load_test_data()
        prio = set(metrics["ZONE_PRIORITIZATION"].dropna().unique())
        self.assertLessEqual(prio, {"High Priority", "Prioritized", "Not Prioritized"})

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_l0w_values_in_range(self):
        """Most metric values (except Orders) should be between 0 and 1."""
        metrics, _ = load_test_data()
        # Use Lead Penetration as a representative bounded metric
        lp = metrics[metrics["METRIC"] == "Lead Penetration"]["L0W"].dropna()
        self.assertTrue((lp >= 0).all(), "Negative Lead Penetration values found")
        # Most values should be ≤ 1, allow some outliers
        self.assertGreater((lp <= 1).mean(), 0.95)


# ──────────────────────────────────────────────────────────────────────────────
# 2. Insights Engine Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestInsightsEngine(unittest.TestCase):
    """Tests for app/insights/engine.py"""

    def _count_consecutive(self, values, higher_is_better):
        """Inline copy of the engine helper for isolated testing."""
        c = 0
        for i in range(len(values) - 1, 0, -1):
            if higher_is_better:
                if values[i] < values[i - 1]: c += 1
                else: break
            else:
                if values[i] > values[i - 1]: c += 1
                else: break
        return c

    def test_consecutive_deterioration_all_down(self):
        vals = [0.9, 0.88, 0.85, 0.82, 0.80, 0.78, 0.75, 0.73, 0.70]
        result = self._count_consecutive(vals, higher_is_better=True)
        self.assertEqual(result, 8)

    def test_consecutive_deterioration_recent_only(self):
        # idx: 0=L8W,1=L7W,2=L6W,3=L5W,4=L4W,5=L3W,6=L2W,7=L1W,8=L0W
        # 0.9→0.92 (up), then 0.92→0.91→0.90→0.88→0.86→0.83→0.80→0.78 (7 downs)
        vals = [0.9, 0.92, 0.91, 0.90, 0.88, 0.86, 0.83, 0.80, 0.78]
        result = self._count_consecutive(vals, higher_is_better=True)
        self.assertEqual(result, 7)  # 7 consecutive trailing drops

    def test_consecutive_no_deterioration(self):
        vals = [0.7, 0.72, 0.75, 0.78, 0.80, 0.82, 0.85, 0.88, 0.90]
        result = self._count_consecutive(vals, higher_is_better=True)
        self.assertEqual(result, 0)

    def test_consecutive_lower_is_better(self):
        """Markdowns/GMV: lower is better, so consecutive increases = deterioration."""
        # idx: 0.10→0.09→0.08 (drops=good), then 0.08→0.11→0.12→0.13→0.14→0.15→0.16 (6 rises=bad)
        vals = [0.10, 0.09, 0.08, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16]
        result = self._count_consecutive(vals, higher_is_better=False)
        self.assertEqual(result, 6)  # 6 consecutive trailing increases

    def test_consecutive_single_element(self):
        result = self._count_consecutive([0.5], higher_is_better=True)
        self.assertEqual(result, 0)

    def test_consecutive_flat(self):
        vals = [0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8]
        result = self._count_consecutive(vals, higher_is_better=True)
        self.assertEqual(result, 0)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_anomaly_detection_returns_insights(self):
        from demo_insights import detect_anomalies, load_data
        metrics, _ = load_data(DATA_PATH)
        insights = detect_anomalies(metrics, limit=10)
        self.assertGreater(len(insights), 0, "Should detect at least one anomaly")
        for ins in insights:
            self.assertIn(ins.severity, {"critical", "warning", "info"})
            self.assertEqual(ins.category, "anomaly")
            self.assertIn("pct_change", ins.data)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_trend_detection_returns_insights(self):
        from demo_insights import detect_trends, load_data
        metrics, _ = load_data(DATA_PATH)
        insights = detect_trends(metrics, limit=10)
        self.assertGreater(len(insights), 0)
        for ins in insights:
            self.assertEqual(ins.category, "trend")
            self.assertGreaterEqual(ins.data["consecutive_weeks"], 3)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_benchmark_detection_returns_insights(self):
        from demo_insights import detect_benchmarks, load_data
        metrics, _ = load_data(DATA_PATH)
        insights = detect_benchmarks(metrics, limit=10)
        self.assertGreater(len(insights), 0)
        for ins in insights:
            self.assertEqual(ins.category, "benchmark")
            self.assertIn("z_score", ins.data)
            self.assertLess(ins.data["z_score"], -1.0)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_opportunity_detection(self):
        from demo_insights import detect_opportunities, load_data
        metrics, orders = load_data(DATA_PATH)
        insights = detect_opportunities(metrics, orders, limit=10)
        for ins in insights:
            self.assertEqual(ins.category, "opportunity")
            # Lead penetration should be below threshold
            self.assertLess(ins.data["lead_penetration"], 0.60)

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_all_insights_have_required_fields(self):
        from demo_insights import (
            detect_anomalies, detect_trends, detect_benchmarks,
            detect_correlations, detect_opportunities, load_data
        )
        metrics, orders = load_data(DATA_PATH)
        all_insights = (
            detect_anomalies(metrics, 5) +
            detect_trends(metrics, 5) +
            detect_benchmarks(metrics, 5) +
            detect_correlations(metrics, 3) +
            detect_opportunities(metrics, orders, 3)
        )
        for ins in all_insights:
            self.assertTrue(ins.title, f"Insight missing title: {ins}")
            self.assertTrue(ins.description, f"Insight missing description: {ins}")
            self.assertTrue(ins.recommendation, f"Insight missing recommendation: {ins}")
            self.assertIn(ins.severity, {"critical", "warning", "info"})
            self.assertIn(ins.category, {"anomaly","trend","benchmark","correlation","opportunity"})


# ──────────────────────────────────────────────────────────────────────────────
HAS_DUCKDB = False
try:
    import duckdb as _duckdb
    HAS_DUCKDB = True
except ImportError:
    pass


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
# 3. SQL Tools Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestSQLTools(unittest.TestCase):
    """Tests for app/bot/tools.py — sql_query function using DuckDB."""

    @classmethod
    def setUpClass(cls):
        """Build an in-memory DuckDB with test data."""
        import duckdb
        cls.conn = duckdb.connect(":memory:")
        if DATA_AVAILABLE:
            metrics = pd.read_excel(DATA_PATH, sheet_name="RAW_INPUT_METRICS")
            orders  = pd.read_excel(DATA_PATH, sheet_name="RAW_ORDERS")
            metrics.rename(columns=dict(zip(WEEK_OLD, WEEK_NEW)), inplace=True)
            cls.conn.register("metrics_wide", metrics)
            cls.conn.register("orders_wide", orders)
        else:
            # Synthetic minimal test data
            metrics = pd.DataFrame({
                "COUNTRY": ["CO","CO","MX"],
                "CITY": ["Bogota","Bogota","CDMX"],
                "ZONE": ["Chapinero","Usaquen","Centro"],
                "ZONE_TYPE": ["Wealthy","Non Wealthy","Non Wealthy"],
                "ZONE_PRIORITIZATION": ["High Priority","Prioritized","Not Prioritized"],
                "METRIC": ["Perfect Orders","Perfect Orders","Lead Penetration"],
                "L8W": [0.90, 0.85, 0.60],
                "L7W": [0.89, 0.84, 0.62],
                "L6W": [0.88, 0.83, 0.64],
                "L5W": [0.87, 0.82, 0.66],
                "L4W": [0.86, 0.81, 0.68],
                "L3W": [0.85, 0.80, 0.70],
                "L2W": [0.84, 0.79, 0.72],
                "L1W": [0.83, 0.78, 0.74],
                "L0W": [0.82, 0.77, 0.76],
            })
            cls.conn.register("metrics_wide", metrics)
        cls.conn.execute("CREATE OR REPLACE VIEW orders_wide AS SELECT * FROM metrics_wide LIMIT 0")

    def _query(self, sql):
        return self.conn.execute(sql).df()

    def test_basic_select(self):
        df = self._query("SELECT COUNT(*) as n FROM metrics_wide")
        self.assertGreater(df["n"].iloc[0], 0)

    def test_filter_by_country(self):
        df = self._query("SELECT DISTINCT COUNTRY FROM metrics_wide WHERE COUNTRY = 'CO'")
        self.assertEqual(len(df), 1)
        self.assertEqual(df["COUNTRY"].iloc[0], "CO")

    def test_aggregation_by_metric(self):
        df = self._query("""
            SELECT METRIC, AVG(L0W) as avg_val, COUNT(*) as n
            FROM metrics_wide
            GROUP BY METRIC
            ORDER BY avg_val DESC
        """)
        self.assertGreater(len(df), 0)
        self.assertIn("avg_val", df.columns)

    def test_week_over_week_change(self):
        df = self._query("""
            SELECT ZONE,
                   L1W, L0W,
                   (L0W - L1W) / NULLIF(L1W, 0) * 100 AS pct_change
            FROM metrics_wide
            WHERE METRIC = 'Perfect Orders'
            ORDER BY pct_change ASC
            LIMIT 5
        """)
        self.assertIn("pct_change", df.columns)

    def test_top_n_zones_query(self):
        df = self._query("""
            SELECT ZONE, COUNTRY, L0W
            FROM metrics_wide
            WHERE METRIC = 'Lead Penetration'
            ORDER BY L0W DESC
            LIMIT 5
        """)
        self.assertLessEqual(len(df), 5)

    def test_cross_metric_join(self):
        """Test the multi-metric query pattern the bot uses."""
        df = self._query("""
            SELECT a.ZONE, a.COUNTRY,
                   a.L0W AS lead_pen,
                   b.L0W AS perfect_orders
            FROM metrics_wide a
            JOIN metrics_wide b ON a.ZONE = b.ZONE AND a.COUNTRY = b.COUNTRY
            WHERE a.METRIC = 'Lead Penetration'
              AND b.METRIC = 'Perfect Orders'
            LIMIT 10
        """)
        self.assertIn("lead_pen", df.columns)
        self.assertIn("perfect_orders", df.columns)

    def test_invalid_sql_does_not_crash(self):
        """Invalid SQL should raise an exception, not return garbage."""
        with self.assertRaises(Exception):
            self._query("SELECT nonexistent_column FROM nonexistent_table")


# ──────────────────────────────────────────────────────────────────────────────
# 4. Utils Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestUtils(unittest.TestCase):
    """Tests for app/utils/helpers.py"""

    def setUp(self):
        from app.utils.helpers import fmt_pct, fmt_num, fmt_delta, SimpleCache, validate_metric, validate_country
        self.fmt_pct = fmt_pct
        self.fmt_num = fmt_num
        self.fmt_delta = fmt_delta
        self.SimpleCache = SimpleCache
        self.validate_metric = validate_metric
        self.validate_country = validate_country

    def test_fmt_pct_normal(self):
        self.assertEqual(self.fmt_pct(0.875), "87.5%")

    def test_fmt_pct_zero(self):
        self.assertEqual(self.fmt_pct(0.0), "0.0%")

    def test_fmt_pct_nan(self):
        self.assertEqual(self.fmt_pct(float("nan")), "N/A")

    def test_fmt_pct_none(self):
        self.assertEqual(self.fmt_pct(None), "N/A")

    def test_fmt_num(self):
        self.assertEqual(self.fmt_num(12345.678), "12,345.68")

    def test_fmt_delta_positive(self):
        result = self.fmt_delta(0.90, 0.80)
        self.assertIn("+", result)
        self.assertIn("12.5", result)

    def test_fmt_delta_negative(self):
        result = self.fmt_delta(0.70, 0.80)
        self.assertIn("-", result)

    def test_fmt_delta_zero_denominator(self):
        self.assertEqual(self.fmt_delta(0.5, 0), "N/A")

    def test_cache_set_get(self):
        cache = self.SimpleCache(default_ttl=10)
        cache.set("key1", {"data": 42})
        result = cache.get("key1")
        self.assertEqual(result, {"data": 42})

    def test_cache_miss(self):
        cache = self.SimpleCache()
        self.assertIsNone(cache.get("nonexistent"))

    def test_cache_expiry(self):
        import time
        cache = self.SimpleCache(default_ttl=1)
        cache.set("temp", "value", ttl=0)  # TTL=0 means immediately expired
        time.sleep(0.01)
        # With TTL 0, expires_at = now + 0 = now, so it should be expired
        # But since we set time.time() + 0, it may still be valid for a tiny window
        # Use a definite past time instead
        cache._store["temp"] = ("value", time.time() - 1)
        self.assertIsNone(cache.get("temp"))

    def test_cache_make_key_deterministic(self):
        cache = self.SimpleCache()
        k1 = cache.make_key("func", "arg1", kwarg="val")
        k2 = cache.make_key("func", "arg1", kwarg="val")
        self.assertEqual(k1, k2)

    def test_validate_metric_exact(self):
        result = self.validate_metric("Perfect Orders")
        self.assertEqual(result, "Perfect Orders")

    def test_validate_metric_fuzzy(self):
        result = self.validate_metric("lead penetration")
        self.assertEqual(result, "Lead Penetration")

    def test_validate_metric_unknown(self):
        result = self.validate_metric("this metric does not exist xyz")
        self.assertIsNone(result)

    def test_validate_country_valid(self):
        self.assertEqual(self.validate_country("co"), "CO")
        self.assertEqual(self.validate_country("MX"), "MX")

    def test_validate_country_invalid(self):
        self.assertIsNone(self.validate_country("ZZ"))


# ──────────────────────────────────────────────────────────────────────────────
# 5. Report Generation Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestReportGeneration(unittest.TestCase):
    """Tests for demo_insights.py HTML generation."""

    @unittest.skipUnless(DATA_AVAILABLE, "Excel file not found")
    def test_generate_html_report(self):
        from demo_insights import (
            detect_anomalies, detect_trends, generate_html_report, load_data
        )
        import tempfile

        metrics, orders = load_data(DATA_PATH)
        insights = detect_anomalies(metrics, 3) + detect_trends(metrics, 3)

        html = generate_html_report(insights)

        self.assertIn("<!DOCTYPE html>", html)
        self.assertIn("Rappi Analytics", html)
        self.assertIn("anomaly", html)
        self.assertGreater(len(html), 5000)

    def test_html_report_with_empty_insights(self):
        from demo_insights import generate_html_report
        html = generate_html_report([])
        self.assertIn("<!DOCTYPE html>", html)
        self.assertIn("0</div><div class=\"lbl\">Alertas cr", html)

    def test_html_contains_filter_buttons(self):
        from demo_insights import generate_html_report
        html = generate_html_report([])
        self.assertIn("filter(", html)
        self.assertIn("anomaly", html)
        self.assertIn("opportunity", html)


# ──────────────────────────────────────────────────────────────────────────────
# 6. Prompt Structure Tests
# ──────────────────────────────────────────────────────────────────────────────

class TestPrompts(unittest.TestCase):
    """Tests for app/bot/prompts.py"""

    def test_system_prompt_has_schema_placeholder(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("{schema}", SYSTEM_PROMPT)

    def test_system_prompt_has_glossary_placeholder(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("{glossary}", SYSTEM_PROMPT)

    def test_system_prompt_contains_business_context(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("Lead Penetration", SYSTEM_PROMPT)
        self.assertIn("Perfect Orders", SYSTEM_PROMPT)
        self.assertIn("Gross Profit", SYSTEM_PROMPT)

    def test_system_prompt_contains_react_pattern(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("Thought", SYSTEM_PROMPT)
        self.assertIn("Action", SYSTEM_PROMPT)
        self.assertIn("Observation", SYSTEM_PROMPT)

    def test_system_prompt_contains_trend_guidance(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("WEEK_OFFSET", SYSTEM_PROMPT)
        self.assertIn("VALUE", SYSTEM_PROMPT)

    def test_system_prompt_contains_benchmark_guidance(self):
        from app.bot.prompts import SYSTEM_PROMPT
        self.assertIn("Wealthy", SYSTEM_PROMPT)
        self.assertIn("Non-Wealthy", SYSTEM_PROMPT) or self.assertIn("Non Wealthy", SYSTEM_PROMPT)

    def test_system_prompt_format_with_schema_and_glossary(self):
        from app.bot.prompts import SYSTEM_PROMPT, METRIC_GLOSSARY
        formatted = SYSTEM_PROMPT.format(schema="TEST SCHEMA", glossary=METRIC_GLOSSARY)
        self.assertIn("TEST SCHEMA", formatted)
        self.assertNotIn("{schema}", formatted)
        self.assertNotIn("{glossary}", formatted)

    def test_metric_glossary_has_all_metrics(self):
        from app.bot.prompts import METRIC_GLOSSARY
        self.assertIn("Lead Penetration", METRIC_GLOSSARY)
        self.assertIn("Perfect Orders", METRIC_GLOSSARY)
        self.assertIn("KEY QUALITY KPI", METRIC_GLOSSARY)
        self.assertIn("Gross Profit UE", METRIC_GLOSSARY)
        self.assertIn("Turbo Adoption", METRIC_GLOSSARY)
        self.assertIn("Restaurants Markdowns / GMV", METRIC_GLOSSARY)

    def test_followup_prompt_has_context_placeholder(self):
        from app.bot.prompts import FOLLOWUP_PROMPT
        self.assertIn("{context}", FOLLOWUP_PROMPT)

    def test_sql_fix_prompt_has_all_placeholders(self):
        from app.bot.prompts import SQL_FIX_PROMPT
        self.assertIn("{sql}", SQL_FIX_PROMPT)
        self.assertIn("{error}", SQL_FIX_PROMPT)
        self.assertIn("{schema}", SQL_FIX_PROMPT)

    def test_insight_narrative_prompt_has_placeholders(self):
        from app.bot.prompts import INSIGHT_NARRATIVE_PROMPT
        self.assertIn("{language}", INSIGHT_NARRATIVE_PROMPT)
        self.assertIn("{insights_json}", INSIGHT_NARRATIVE_PROMPT)


# ------------------------------------------------------------------------------
# 7. Cost Monitor Tests
# ------------------------------------------------------------------------------

class TestCostMonitor(unittest.TestCase):
    """Tests for app/utils/cost_monitor.py"""

    def setUp(self):
        from app.utils.cost_monitor import CostMonitor
        self.monitor = CostMonitor()

    def test_record_call_returns_record(self):
        record = self.monitor.record_call("claude-sonnet-4-5", 1000, 200)
        self.assertEqual(record.model, "claude-sonnet-4-5")
        self.assertEqual(record.input_tokens, 1000)
        self.assertEqual(record.output_tokens, 200)
        self.assertGreater(record.estimated_cost_usd, 0)

    def test_session_total_accumulates(self):
        self.monitor.record_call("claude-sonnet-4-5", 1000, 200)
        self.monitor.record_call("claude-sonnet-4-5", 500, 100)
        totals = self.monitor.session_total()
        self.assertEqual(totals["total_calls"], 2)
        self.assertEqual(totals["total_input_tokens"], 1500)
        self.assertEqual(totals["total_output_tokens"], 300)
        self.assertEqual(totals["total_tokens"], 1800)
        self.assertGreater(totals["estimated_cost_usd"], 0)

    def test_session_report_string(self):
        self.monitor.record_call("claude-sonnet-4-5", 500, 100)
        report = self.monitor.session_report()
        self.assertIn("API Cost Report", report)
        self.assertIn("Total API calls", report)
        self.assertIn("$", report)

    def test_cost_calculation_known_model(self):
        from app.utils.cost_monitor import MODEL_PRICING
        # claude-sonnet-4-5: $3/M input, $15/M output
        record = self.monitor.record_call("claude-sonnet-4-5", 1_000_000, 1_000_000)
        expected = 3.00 + 15.00
        self.assertAlmostEqual(record.estimated_cost_usd, expected, places=2)

    def test_cost_calculation_unknown_model_uses_default(self):
        record = self.monitor.record_call("unknown-model-xyz", 1000, 200)
        self.assertGreater(record.estimated_cost_usd, 0)

    def test_reset_clears_calls(self):
        self.monitor.record_call("claude-sonnet-4-5", 100, 50)
        self.monitor.reset()
        totals = self.monitor.session_total()
        self.assertEqual(totals["total_calls"], 0)
        self.assertEqual(totals["total_tokens"], 0)

    def test_calls_property_returns_list(self):
        self.monitor.record_call("claude-sonnet-4-5", 100, 50)
        calls = self.monitor.calls
        self.assertEqual(len(calls), 1)
        self.assertIsInstance(calls, list)

    def test_empty_session_total(self):
        totals = self.monitor.session_total()
        self.assertEqual(totals["total_calls"], 0)
        self.assertEqual(totals["estimated_cost_usd"], 0)


# ------------------------------------------------------------------------------
# 8. Intent Classification Tests
# ------------------------------------------------------------------------------

class TestIntentClassification(unittest.TestCase):
    """Tests for the classify_intent function in agent.py"""

    def setUp(self):
        try:
            from app.bot.agent import classify_intent
        except ImportError:
            self.skipTest("anthropic module not installed")
        self.classify = classify_intent

    def test_trend_intent(self):
        self.assertEqual(self.classify("Show me the trend of Perfect Orders"), "trend")

    def test_comparison_intent(self):
        self.assertEqual(self.classify("Compare Wealthy vs Non Wealthy zones"), "comparison")

    def test_anomaly_intent(self):
        self.assertEqual(self.classify("Any anomalies in the data this week?"), "anomaly")

    def test_general_intent(self):
        self.assertEqual(self.classify("What is the average value?"), "general")

    def test_spanish_trend(self):
        self.assertEqual(self.classify("Muestra la tendencia de Perfect Orders"), "trend")

    def test_spanish_comparison(self):
        self.assertEqual(self.classify("Comparar zonas wealthy y no wealthy"), "comparison")


# ------------------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------------------

def run_tests():
    """Standalone runner -- prints a clean summary without pytest."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestDataLoader,
        TestInsightsEngine,
        TestSQLTools,
        TestUtils,
        TestReportGeneration,
        TestPrompts,
        TestCostMonitor,
        TestIntentClassification,
    ]

    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    print("\n" + "="*60)
    print(f"Tests run:    {result.testsRun}")
    print(f"Failures:     {len(result.failures)}")
    print(f"Errors:       {len(result.errors)}")
    print(f"Skipped:      {len(result.skipped)}")
    status = "ALL PASSED" if result.wasSuccessful() else "FAILURES DETECTED"
    print(f"Status:       {status}")
    print("="*60)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests())

