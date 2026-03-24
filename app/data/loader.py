"""
Data Loader: reads the Rappi Excel file, normalises it into long format
using vectorized pd.melt(), and registers DuckDB tables for fast SQL
querying throughout the application.

Design decision: The canonical analytical table is the long-format
representation (metrics_long / orders_long / all_data_long) with columns
WEEK_OFFSET (int, 0=oldest L8W, 8=current L0W) and VALUE. This format
is non-negotiable for correct time-series analysis.
"""

import os
import logging
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Original wide-format column names coming from the Excel file.
WEEK_COLS_METRICS = [
    "L8W_ROLL", "L7W_ROLL", "L6W_ROLL", "L5W_ROLL",
    "L4W_ROLL", "L3W_ROLL", "L2W_ROLL", "L1W_ROLL", "L0W_ROLL",
]

WEEK_COLS_ORDERS = ["L8W", "L7W", "L6W", "L5W", "L4W", "L3W", "L2W", "L1W", "L0W"]

# Standardised short labels after renaming (used in the wide tables).
WEEK_LABELS = ["L8W", "L7W", "L6W", "L5W", "L4W", "L3W", "L2W", "L1W", "L0W"]

# Mapping from label to integer offset (0 = oldest, 8 = current week).
WEEK_LABEL_TO_OFFSET: dict[str, int] = {label: idx for idx, label in enumerate(WEEK_LABELS)}


class DataLoader:
    """Singleton-style loader: reads Excel once, exposes a DuckDB connection."""

    def __init__(self, excel_path: str):
        self.excel_path = Path(excel_path)
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        self._metrics_df: pd.DataFrame | None = None
        self._orders_df: pd.DataFrame | None = None
        self._long_metrics_df: pd.DataFrame | None = None
        self._long_orders_df: pd.DataFrame | None = None
        self._loaded = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Read the Excel workbook and register all DuckDB tables."""
        if self._loaded:
            return
        logger.info("Loading data from %s", self.excel_path)

        # Explicitly load specific sheets by name
        self._metrics_df = pd.read_excel(self.excel_path, sheet_name="RAW_INPUT_METRICS")
        self._orders_df = pd.read_excel(self.excel_path, sheet_name="RAW_ORDERS")

        # Optionally read RAW_SUMMARY if needed in the future, but unused in tables
        try:
            pd.read_excel(self.excel_path, sheet_name="RAW_SUMMARY")
        except Exception:
            pass

        # Strict validation
        if self._metrics_df is None or self._metrics_df.empty:
            raise ValueError("Validation Failed: metrics_wide dataset is empty.")
        if self._orders_df is None or self._orders_df.empty:
            raise ValueError("Validation Failed: orders_wide dataset is empty.")

        self._normalize()
        self._register_tables()
        self._loaded = True
        logger.info(
            "Loaded %d metric rows, %d order rows",
            len(self._metrics_df),
            len(self._orders_df),
        )

    def query(self, sql: str) -> pd.DataFrame:
        """Execute arbitrary SQL against the in-memory DuckDB and return a DataFrame."""
        return self.conn.execute(sql).df()

    @property
    def metrics_df(self) -> pd.DataFrame:
        return self._metrics_df

    @property
    def orders_df(self) -> pd.DataFrame:
        return self._orders_df

    @property
    def long_metrics_df(self) -> pd.DataFrame:
        return self._long_metrics_df

    # ------------------------------------------------------------------
    # Internal: Normalization
    # ------------------------------------------------------------------

    def _normalize(self) -> None:
        """Rename week columns dynamically, merge metadata to orders, and
        build the long-format tables using vectorized pd.melt()."""

        # --- Metrics: dynamically detect and strip suffixes (like _ROLL) ---
        self._metrics_df.columns = [
            str(c).replace("_ROLL", "").replace("_roll", "") 
            for c in self._metrics_df.columns
        ]

        # --- Meta-Merge: Enrich Orders with TYPE and PRIORITIZATION ------
        metadata = self._metrics_df[["COUNTRY", "CITY", "ZONE", "ZONE_TYPE", "ZONE_PRIORITIZATION"]].drop_duplicates(subset=["COUNTRY", "CITY", "ZONE"])
        logger.info("Duplicados eliminados en metadata de zonas.")
        self._orders_df = self._orders_df.merge(
            metadata, on=["COUNTRY", "CITY", "ZONE"], how="left"
        )
        nulls = self._orders_df["ZONE_TYPE"].isna().sum()
        if nulls > 0:
            logger.warning("Meta-Merge left %d rows in orders_df without ZONE_TYPE metadata.", nulls)

        # --- Vectorized wide-to-long pivot for METRICS ----------------------
        id_cols = ["COUNTRY", "CITY", "ZONE", "ZONE_TYPE", "ZONE_PRIORITIZATION", "METRIC"]
        self._long_metrics_df = pd.melt(
            self._metrics_df,
            id_vars=id_cols,
            value_vars=WEEK_LABELS,
            var_name="WEEK",
            value_name="VALUE",
        )
        self._long_metrics_df["WEEK_OFFSET"] = (
            self._long_metrics_df["WEEK"].map(WEEK_LABEL_TO_OFFSET)
        )

        # --- Vectorized wide-to-long pivot for ORDERS -----------------------
        order_id_cols = ["COUNTRY", "CITY", "ZONE", "ZONE_TYPE", "ZONE_PRIORITIZATION"]
        self._long_orders_df = pd.melt(
            self._orders_df,
            id_vars=order_id_cols,
            value_vars=WEEK_COLS_ORDERS,
            var_name="WEEK",
            value_name="VALUE",
        )
        self._long_orders_df["METRIC"] = "Orders"
        self._long_orders_df["WEEK_OFFSET"] = (
            self._long_orders_df["WEEK"].map(WEEK_LABEL_TO_OFFSET)
        )

    # ------------------------------------------------------------------
    # Internal: DuckDB Registration
    # ------------------------------------------------------------------

    def _register_tables(self) -> None:
        """Register all DataFrames as DuckDB views for SQL access."""
        self.conn.register("metrics_wide", self._metrics_df)
        self.conn.register("orders_wide", self._orders_df)
        self.conn.register("metrics_long", self._long_metrics_df)
        self.conn.register("orders_long", self._long_orders_df)

        # Unified long table: metrics + orders together.
        combined = pd.concat(
            [self._long_metrics_df, self._long_orders_df], ignore_index=True
        )
        self.conn.register("all_data_long", combined)
        logger.info(
            "DuckDB tables registered: metrics_wide, orders_wide, "
            "metrics_long, orders_long, all_data_long"
        )

    # ------------------------------------------------------------------
    # Schema description (injected into LLM prompts)
    # ------------------------------------------------------------------

    def schema_description(self) -> str:
        """Return a compact schema description for LLM prompts."""
        return """
== DuckDB Tables Available ==

1. metrics_wide  -- one row per (COUNTRY, CITY, ZONE, ZONE_TYPE, ZONE_PRIORITIZATION, METRIC)
   Columns: COUNTRY, CITY, ZONE, ZONE_TYPE, ZONE_PRIORITIZATION, METRIC,
            L8W, L7W, L6W, L5W, L4W, L3W, L2W, L1W, L0W
   (L0W = current week, L8W = 8 weeks ago; values are decimals for %, integers for Orders)

2. orders_wide  -- same structure but METRIC is always "Orders" (integer counts)
   Columns: COUNTRY, CITY, ZONE, L8W ... L0W

3. metrics_long -- CANONICAL long-format pivot of metrics_wide (use for trends and time-series)
   Columns: COUNTRY, CITY, ZONE, ZONE_TYPE, ZONE_PRIORITIZATION, METRIC,
            WEEK (L0W..L8W), WEEK_OFFSET (int 0=L8W oldest .. 8=L0W current), VALUE

4. orders_long  -- long-format pivot of orders_wide
   Columns: COUNTRY, CITY, ZONE, METRIC ("Orders"), WEEK, WEEK_OFFSET, VALUE

5. all_data_long -- union of metrics_long + orders_long (all metrics in one table)

== IMPORTANT: For trend / time-series queries use metrics_long or all_data_long ==
   ORDER BY WEEK_OFFSET ASC for chronological order.
   WEEK_OFFSET 0 = 8 weeks ago (L8W), WEEK_OFFSET 8 = current week (L0W).

== Countries ==
AR, BR, CL, CO, CR, EC, MX, PE, UY

== Metrics (in metrics_wide / metrics_long) ==
- % PRO Users Who Breakeven
- % Restaurants Sessions With Optimal Assortment
- Gross Profit UE
- Lead Penetration
- MLTV Top Verticals Adoption
- Non-Pro PTC > OP
- Perfect Orders   ** KEY QUALITY KPI **
- Pro Adoption (Last Week Status)
- Restaurants Markdowns / GMV
- Restaurants SS > ATC CVR
- Restaurants SST > SS CVR
- Retail SST > SS CVR
- Turbo Adoption

== Notes ==
- ZONE_TYPE: 'Wealthy' | 'Non Wealthy'
- ZONE_PRIORITIZATION: 'High Priority' | 'Prioritized' | 'Not Prioritized'
- Most metric values are proportions (0.0-1.0). Orders are integer counts.
- L0W / WEEK_OFFSET=8 is the CURRENT week. L8W / WEEK_OFFSET=0 is 8 weeks ago.
"""


@lru_cache(maxsize=1)
def get_loader() -> DataLoader:
    """Module-level singleton: call get_loader() anywhere to get the loaded instance."""
    from dotenv import load_dotenv
    load_dotenv()  # Carga explícitamente el archivo .env

    # 1. Tratamos de leer la ruta directamente
    env_path = os.getenv("DATA_PATH")
    base_dir = Path(__file__).resolve().parent.parent.parent
    
    if env_path:
        # Si la variable de entorno dice "/app/..." pero estamos localmente, fallará
        # Por lo tanto, si la ruta enviada no existe, pero empieza por "data/...", usamos una ruta segura
        raw_path = Path(env_path)
        if raw_path.is_absolute() and raw_path.exists():
            data_path = raw_path
        elif not raw_path.is_absolute() and (base_dir / raw_path).exists():
            data_path = base_dir / raw_path
        else:
            # Fallback forzado en caso de arrastrar variables fantasma del Docker
            data_path = base_dir / "data" / "raw" / "rappi_data.xlsx"
    else:
        # 2. Ruta estricta relativa al archivo actual
        data_path = base_dir / "data" / "raw" / "rappi_data.xlsx"
        
    if not data_path.exists():
        logger.error("Critical Error: File not found at %s", data_path)
        raise FileNotFoundError(f"¡Atención! No se encontró el dataset en: {data_path}. Asegúrate de que el archivo exista.")

    loader = DataLoader(str(data_path))
    loader.load()
    return loader