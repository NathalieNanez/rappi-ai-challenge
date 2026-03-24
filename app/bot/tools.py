"""
Tools available to the conversational agent.
Each tool is a plain function that the agent executor calls.
Outputs use pydantic models for structured, validated responses.
"""

import json
import logging
import re
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
from pydantic import BaseModel, Field

from app.data.loader import get_loader

logger = logging.getLogger(__name__)

MAX_ROWS_IN_RESPONSE = 50  # cap to avoid token overflow

# ---------------------------------------------------------------------------
# Rappi brand palette for professional chart styling.
# ---------------------------------------------------------------------------
RAPPI_COLORS = [
    "#FF441F",  # Rappi red (primary)
    "#1E293B",  # Dark slate
    "#3B82F6",  # Blue
    "#10B981",  # Emerald
    "#F59E0B",  # Amber
    "#8B5CF6",  # Violet
    "#EC4899",  # Pink
    "#6366F1",  # Indigo
]

RAPPI_LAYOUT = dict(
    font_family="Inter, sans-serif",
    title_font_size=16,
    legend_title_font_size=12,
    margin=dict(l=40, r=20, t=60, b=60),
    template="plotly_white",
    colorway=RAPPI_COLORS,
)

# ---------------------------------------------------------------------------
# Chart-type semantic keywords for smart routing.
# ---------------------------------------------------------------------------
COMPARISON_KEYWORDS = [
    "compare", "comparar", "vs", "versus", "ranking", "rank", "top",
    "bottom", "best", "worst", "by country", "por país", "benchmark",
    "wealthy", "non wealthy", "distribution",
]
TREND_KEYWORDS = [
    "trend", "tendencia", "over time", "evolution", "evolución",
    "weekly", "semanal", "time series", "historical", "progression",
    "week_offset", "l8w", "l0w",
]


# ---------------------------------------------------------------------------
# Pydantic models for structured tool outputs
# ---------------------------------------------------------------------------

class SQLQueryResult(BaseModel):
    """Structured output from sql_query tool."""
    ok: bool
    data: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    error: str = ""
    auto_fixed: bool = False


class ChartResult(BaseModel):
    """Structured output from generate_chart tool."""
    ok: bool
    figure_json: str = ""
    row_count: int = 0
    chart_type_used: str = ""
    error: str = ""


class ContextInfoResult(BaseModel):
    """Structured output from get_context_info tool."""
    ok: bool
    countries: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)
    matching_zones: list[str] = Field(default_factory=list)
    error: str = ""


# ---------------------------------------------------------------------------
# Smart chart-type routing
# ---------------------------------------------------------------------------

def _infer_chart_type(spec: dict[str, Any]) -> str:
    """
    Infer the best chart type from the spec context.
    Rules:
      - If description/title/sql contains comparison semantics -> bar
      - If description/title/sql contains trend semantics -> line
      - Otherwise, use the explicitly requested type.
    """
    requested = spec.get("type", "bar")
    text_context = " ".join([
        spec.get("title", ""),
        spec.get("sql", ""),
        spec.get("description", ""),
    ]).lower()

    comparison_score = sum(1 for kw in COMPARISON_KEYWORDS if kw in text_context)
    trend_score = sum(1 for kw in TREND_KEYWORDS if kw in text_context)

    if trend_score > comparison_score and trend_score > 0:
        inferred = "line"
    elif comparison_score > trend_score and comparison_score > 0:
        inferred = "bar"
    else:
        inferred = requested

    if inferred != requested:
        logger.info(
            "Smart chart routing: overrode '%s' -> '%s' (trend=%d, comp=%d)",
            requested, inferred, trend_score, comparison_score,
        )
    return inferred


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

_LAST_QUERY_DF: pd.DataFrame | None = None

def sql_query(sql: str) -> dict[str, Any]:
    """
    Execute a SQL query against the DuckDB in-memory database.
    Returns a structured result dictionary compatible with SQLQueryResult.
    """
    global _LAST_QUERY_DF
    loader = get_loader()
    sql = sql.strip().rstrip(";")

    try:
        df = loader.query(sql)
        _LAST_QUERY_DF = df.copy()

        truncated = False
        if len(df) > MAX_ROWS_IN_RESPONSE:
            df = df.head(MAX_ROWS_IN_RESPONSE)
            truncated = True

        df = df.where(pd.notna(df), None)

        result = SQLQueryResult(
            ok=True,
            data=df.to_dict(orient="records"),
            columns=list(df.columns),
            row_count=len(df),
            truncated=truncated,
        )
        return result.model_dump()
    except Exception as e:
        logger.error("SQL error: %s\nSQL: %s", e, sql)
        return SQLQueryResult(ok=False, error=str(e)).model_dump()


def generate_chart(spec: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    """
    Generate a Plotly figure from a declarative spec.
    Uses smart chart-type routing: comparisons -> bar, trends -> line.
    All charts use Rappi-branded professional styling.
    """
    if spec is None:
        spec = kwargs

    loader = get_loader()
    try:
        df = loader.query(spec["sql"].strip().rstrip(";"))
        if df.empty:
            return ChartResult(ok=False, error="Query returned no data").model_dump()

        chart_type = _infer_chart_type(spec)
        x = spec.get("x")
        y = spec.get("y")
        color = spec.get("color")
        title = spec.get("title", "")
        orientation = spec.get("orientation", "v")

        fig: go.Figure

        if chart_type == "line":
            fig = px.line(
                df, x=x, y=y, color=color, title=title,
                markers=True,
                template="plotly_white",
                color_discrete_sequence=RAPPI_COLORS,
            )
        elif chart_type == "bar":
            fig = px.bar(
                df, x=x, y=y, color=color, title=title,
                orientation=orientation,
                template="plotly_white",
                text_auto=".2%",
                barmode="group",
                color_discrete_sequence=RAPPI_COLORS,
            )
        elif chart_type == "scatter":
            fig = px.scatter(
                df, x=x, y=y, color=color, title=title,
                template="plotly_white",
                hover_data=df.columns.tolist(),
                color_discrete_sequence=RAPPI_COLORS,
            )
        elif chart_type == "heatmap":
            pivot = df.pivot(
                index=spec.get("index", x),
                columns=spec.get("columns", color),
                values=y,
            )
            fig = go.Figure(go.Heatmap(
                z=pivot.values.tolist(),
                x=list(pivot.columns),
                y=list(pivot.index),
                colorscale="RdYlGn",
            ))
            fig.update_layout(title=title, template="plotly_white")
        elif chart_type == "box":
            fig = px.box(
                df, x=x, y=y, color=color, title=title,
                template="plotly_white",
                color_discrete_sequence=RAPPI_COLORS,
            )
        else:
            return ChartResult(
                ok=False, error=f"Unknown chart type: {chart_type}"
            ).model_dump()

        fig.update_layout(
            **RAPPI_LAYOUT,
            xaxis_title=spec.get("xaxis_title", x),
            yaxis_title=spec.get("yaxis_title", y if isinstance(y, str) else ""),
        )

        result = ChartResult(
            ok=True,
            figure_json=fig.to_json(),
            row_count=len(df),
            chart_type_used=chart_type,
        )
        return result.model_dump()

    except Exception as e:
        logger.error("Chart generation error: %s", e)
        return ChartResult(ok=False, error=str(e)).model_dump()


def get_context_info(query_hint: str = "") -> dict[str, Any]:
    """
    Return summary stats and available filters to help the LLM
    craft accurate SQL (avoids hallucinating zone names, etc.).
    """
    loader = get_loader()
    try:
        countries = loader.query(
            "SELECT DISTINCT COUNTRY FROM metrics_wide ORDER BY COUNTRY"
        )["COUNTRY"].tolist()
        metrics = loader.query(
            "SELECT DISTINCT METRIC FROM metrics_wide ORDER BY METRIC"
        )["METRIC"].tolist()
        cities = loader.query(
            "SELECT DISTINCT CITY FROM metrics_wide ORDER BY CITY"
        )["CITY"].tolist()

        zones: list[str] = []
        if query_hint:
            safe_hint = query_hint.replace("'", "''")
            zones = loader.query(
                f"SELECT DISTINCT ZONE FROM metrics_wide "
                f"WHERE LOWER(ZONE) LIKE LOWER('%{safe_hint}%') LIMIT 10"
            )["ZONE"].tolist()

        result = ContextInfoResult(
            ok=True,
            countries=countries,
            metrics=metrics,
            cities=cities[:30],
            matching_zones=zones,
        )
        return result.model_dump()
    except Exception as e:
        return ContextInfoResult(ok=False, error=str(e)).model_dump()


def export_results() -> dict[str, Any]:
    """
    Export the last queried SQL DataFrame to a temporary CSV buffer.
    """
    global _LAST_QUERY_DF
    if _LAST_QUERY_DF is None or _LAST_QUERY_DF.empty:
        return {"ok": False, "error": "No query data available to export."}
    
    try:
        # Create a temporary buffer and save CSV
        buffer = io.StringIO()
        _LAST_QUERY_DF.to_csv(buffer, index=False)
        csv_str = buffer.getvalue()
        
        return {
            "ok": True,
            "csv_content": csv_str,
            "message": "Data exported successfully to CSV buffer."
        }
    except Exception as e:
        return {"ok": False, "error": f"Failed to export: {str(e)}"}


# Tool registry -- used by the agent to dispatch tool calls.
TOOL_REGISTRY: dict[str, callable] = {
    "sql_query": sql_query,
    "generate_chart": generate_chart,
    "get_context_info": get_context_info,
    "export_results": export_results,
}

# Anthropic tool definitions (passed to the API).
TOOL_DEFINITIONS = [
    {
        "name": "sql_query",
        "description": (
            "Execute a SQL query against the Rappi DuckDB database. "
            "Use this to answer any question about metrics, zones, trends, or rankings. "
            "Always use this before answering a quantitative question. "
            "For TREND queries, use metrics_long or all_data_long with WEEK_OFFSET and VALUE columns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": (
                        "Valid DuckDB SQL query. Tables: metrics_wide, orders_wide, "
                        "metrics_long, orders_long, all_data_long. "
                        "For trends, query WEEK_OFFSET and VALUE from metrics_long."
                    ),
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "generate_chart",
        "description": (
            "Generate a Plotly chart when a trend, comparison, or distribution would be "
            "better understood visually. Smart routing: trend data -> line chart, "
            "comparison data -> bar chart. Always include a SQL query in the spec."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "type":  {"type": "string", "enum": ["bar", "line", "scatter", "heatmap", "box"]},
                "sql":   {"type": "string", "description": "SQL to fetch chart data"},
                "x":     {"type": "string", "description": "Column for x-axis"},
                "y":     {"type": "string", "description": "Column for y-axis"},
                "color": {"type": "string", "description": "Column for color grouping (optional)"},
                "title": {"type": "string"},
                "xaxis_title": {"type": "string"},
                "yaxis_title": {"type": "string"},
                "orientation": {"type": "string", "enum": ["v", "h"]},
            },
            "required": ["type", "sql", "x", "y", "title"],
        },
    },
    {
        "name": "get_context_info",
        "description": (
            "Get available filter values (countries, metrics, cities, zones). "
            "Use this FIRST when the user mentions a specific zone, city, or metric name "
            "to validate it exists before writing SQL."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query_hint": {
                    "type": "string",
                    "description": "Partial zone or city name to search for",
                }
            },
            "required": [],
        },
    },
    {
        "name": "export_results",
        "description": (
            "Saves the current SQLQueryResult to a temporary buffer and returns it as a CSV. "
            "Call this if the user asks to export, download, or save the query numbers."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]
