"""
Reporter: takes a list of Insight objects and generates an executive HTML report.
Uses Jinja2 for the HTML template and Claude for the narrative summary.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import anthropic
from jinja2 import Template

from app.insights.engine import Insight
from app.bot.prompts import INSIGHT_NARRATIVE_PROMPT

logger = logging.getLogger(__name__)

SEVERITY_COLOR = {
    "critical": "#dc2626",
    "warning":  "#d97706",
    "info":     "#2563eb",
}

CATEGORY_LABEL = {
    "anomaly":     "🚨 Anomalía",
    "trend":       "📉 Tendencia",
    "benchmark":   "📊 Benchmark",
    "correlation": "🔗 Correlación",
    "opportunity": "🚀 Oportunidad",
}

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reporte Ejecutivo — Rappi Analytics</title>
<style>
  :root {
    --red:    #dc2626; --amber: #d97706; --blue: #2563eb;
    --green:  #16a34a; --gray:  #6b7280; --bg: #f9fafb;
    --card:   #ffffff; --border: #e5e7eb; --text: #111827;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: Inter, system-ui, sans-serif; background: var(--bg);
         color: var(--text); line-height: 1.6; }
  .page { max-width: 960px; margin: 0 auto; padding: 40px 24px; }
  header { border-bottom: 3px solid var(--red); padding-bottom: 24px; margin-bottom: 32px; }
  header h1 { font-size: 28px; font-weight: 700; color: var(--red); }
  header p  { color: var(--gray); font-size: 14px; margin-top: 4px; }
  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
             gap: 16px; margin-bottom: 32px; }
  .kpi { background: var(--card); border: 1px solid var(--border); border-radius: 12px;
         padding: 20px; text-align: center; }
  .kpi .num { font-size: 36px; font-weight: 700; }
  .kpi .lbl { font-size: 12px; color: var(--gray); margin-top: 4px; }
  .kpi.crit .num { color: var(--red); }
  .kpi.warn .num { color: var(--amber); }
  .kpi.info .num { color: var(--blue); }
  .section { margin-bottom: 40px; }
  .section h2 { font-size: 20px; font-weight: 600; margin-bottom: 16px;
                border-left: 4px solid var(--red); padding-left: 12px; }
  .narrative { background: var(--card); border: 1px solid var(--border);
               border-radius: 12px; padding: 24px; margin-bottom: 32px;
               line-height: 1.8; }
  .narrative h3 { font-size: 16px; font-weight: 600; margin: 16px 0 8px; }
  .narrative p  { margin-bottom: 12px; }
  .narrative ul { padding-left: 20px; }
  .narrative li { margin-bottom: 6px; }
  .card { background: var(--card); border: 1px solid var(--border);
          border-radius: 12px; padding: 20px; margin-bottom: 12px; }
  .card-header { display: flex; align-items: flex-start; gap: 12px; margin-bottom: 8px; }
  .badge { font-size: 11px; font-weight: 600; padding: 3px 8px;
           border-radius: 20px; white-space: nowrap; }
  .badge-critical { background: #fee2e2; color: var(--red); }
  .badge-warning  { background: #fef3c7; color: var(--amber); }
  .badge-info     { background: #dbeafe; color: var(--blue); }
  .card h3 { font-size: 15px; font-weight: 600; line-height: 1.4; }
  .card p  { font-size: 14px; color: #374151; margin: 8px 0; }
  .recommendation { font-size: 13px; background: #f0fdf4; border-left: 3px solid var(--green);
                    border-radius: 0 8px 8px 0; padding: 10px 14px; margin-top: 10px;
                    color: #166534; }
  .cat-filter { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 24px; }
  .cat-btn { font-size: 13px; padding: 6px 14px; border-radius: 20px; border: 1px solid var(--border);
             background: white; cursor: pointer; transition: all .15s; }
  .cat-btn.active, .cat-btn:hover { background: var(--red); color: white; border-color: var(--red); }
  .data-chip { display: inline-block; font-size: 11px; background: #f3f4f6;
               padding: 2px 8px; border-radius: 4px; margin: 2px; font-family: monospace; }
  footer { text-align: center; color: var(--gray); font-size: 12px;
           padding-top: 32px; border-top: 1px solid var(--border); margin-top: 40px; }
</style>
</head>
<body>
<div class="page">

<header>
  <h1>🚦 Reporte Ejecutivo — Rappi Analytics</h1>
  <p>Generado: {{ generated_at }} · {{ total_insights }} hallazgos detectados · 9 semanas de datos</p>
</header>

<!-- KPI SUMMARY -->
<div class="kpi-row">
  <div class="kpi crit">
    <div class="num">{{ critical_count }}</div>
    <div class="lbl">Alertas críticas</div>
  </div>
  <div class="kpi warn">
    <div class="num">{{ warning_count }}</div>
    <div class="lbl">Advertencias</div>
  </div>
  <div class="kpi info">
    <div class="num">{{ opportunity_count }}</div>
    <div class="lbl">Oportunidades</div>
  </div>
  <div class="kpi">
    <div class="num" style="color:#16a34a">{{ zones_affected }}</div>
    <div class="lbl">Zonas afectadas</div>
  </div>
  <div class="kpi">
    <div class="num" style="color:#7c3aed">{{ countries_affected }}</div>
    <div class="lbl">Países con alertas</div>
  </div>
</div>

<!-- EXECUTIVE NARRATIVE -->
<div class="section">
  <h2>Resumen ejecutivo</h2>
  <div class="narrative">
    {{ narrative_html }}
  </div>
</div>

<!-- INSIGHTS DETAIL -->
<div class="section">
  <h2>Hallazgos detallados</h2>
  <div class="cat-filter">
    <button class="cat-btn active" onclick="filterInsights('all')">Todos ({{ total_insights }})</button>
    <button class="cat-btn" onclick="filterInsights('anomaly')">🚨 Anomalías ({{ anomaly_count }})</button>
    <button class="cat-btn" onclick="filterInsights('trend')">📉 Tendencias ({{ trend_count }})</button>
    <button class="cat-btn" onclick="filterInsights('benchmark')">📊 Benchmark ({{ benchmark_count }})</button>
    <button class="cat-btn" onclick="filterInsights('correlation')">🔗 Correlaciones ({{ correlation_count }})</button>
    <button class="cat-btn" onclick="filterInsights('opportunity')">🚀 Oportunidades ({{ opportunity_count }})</button>
  </div>

  <div id="insights-container">
  {% for insight in insights %}
  <div class="card" data-category="{{ insight.category }}" data-severity="{{ insight.severity }}">
    <div class="card-header">
      <span class="badge badge-{{ insight.severity }}">{{ insight.severity|upper }}</span>
      <h3>{{ insight.title }}</h3>
    </div>
    <p>{{ insight.description }}</p>
    {% if insight.data %}
    <div>
      {% for k, v in insight.data.items() %}
      <span class="data-chip">{{ k }}: {{ v }}</span>
      {% endfor %}
    </div>
    {% endif %}
    <div class="recommendation">
      💡 <strong>Recomendación:</strong> {{ insight.recommendation }}
    </div>
  </div>
  {% endfor %}
  </div>
</div>

<footer>
  Rappi Analytics · Sistema de Análisis Inteligente · {{ generated_at }}
</footer>

</div>

<script>
function filterInsights(cat) {
  document.querySelectorAll('.cat-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  document.querySelectorAll('.card').forEach(c => {
    c.style.display = (cat === 'all' || c.dataset.category === cat) ? '' : 'none';
  });
}
</script>
</body>
</html>
"""


class ReportGenerator:
    """Generates the executive HTML report from detected insights."""

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.reports_path = Path(os.getenv("REPORTS_PATH", "/app/reports"))
        self.reports_path.mkdir(parents=True, exist_ok=True)

    def generate(self, insights: list[Insight], language: str = "es") -> Path:
        """Generate HTML report and save to disk. Returns the file path."""
        narrative_md = self._generate_narrative(insights, language)
        narrative_html = self._md_to_html(narrative_md)

        # Aggregate counts
        critical_count   = sum(1 for i in insights if i.severity == "critical")
        warning_count    = sum(1 for i in insights if i.severity == "warning")
        opportunity_count = sum(1 for i in insights if i.category == "opportunity")
        anomaly_count    = sum(1 for i in insights if i.category == "anomaly")
        trend_count      = sum(1 for i in insights if i.category == "trend")
        benchmark_count  = sum(1 for i in insights if i.category == "benchmark")
        correlation_count = sum(1 for i in insights if i.category == "correlation")

        all_zones     = [z for i in insights for z in i.affected_zones]
        zones_affected    = len(set(all_zones))
        countries_affected = len({i.country for i in insights if i.country})

        template = Template(HTML_TEMPLATE)
        html_content = template.render(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
            total_insights=len(insights),
            critical_count=critical_count,
            warning_count=warning_count,
            opportunity_count=opportunity_count,
            anomaly_count=anomaly_count,
            trend_count=trend_count,
            benchmark_count=benchmark_count,
            correlation_count=correlation_count,
            zones_affected=zones_affected,
            countries_affected=countries_affected,
            narrative_html=narrative_html,
            insights=insights,
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.reports_path / f"rappi_insights_{timestamp}.html"
        output_path.write_text(html_content, encoding="utf-8")
        logger.info(f"Report saved: {output_path}")
        return output_path

    def _generate_narrative(self, insights: list[Insight], language: str) -> str:
        """Use Claude to write the executive narrative."""
        # Only pass top 15 insights to keep prompt short.
        top_insights = insights[:15]
        insights_data = [
            i.model_dump(include={"category", "severity", "title", "description", "recommendation", "data"})
            for i in top_insights
        ]
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=2048,
                messages=[{
                    "role": "user",
                    "content": INSIGHT_NARRATIVE_PROMPT.format(
                        language="español" if language == "es" else "English",
                        insights_json=json.dumps(insights_data, ensure_ascii=False, indent=2),
                    )
                }]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Narrative generation failed: {e}")
            return f"## Resumen\n\nSe detectaron {len(insights)} hallazgos en el análisis automático."

    @staticmethod
    def _md_to_html(md_text: str) -> str:
        """Simple markdown → HTML conversion (no heavy deps)."""
        import re
        lines = md_text.split("\n")
        html_lines = []
        in_list = False

        for line in lines:
            # Headers
            if line.startswith("### "):
                if in_list: html_lines.append("</ul>"); in_list = False
                html_lines.append(f"<h3>{line[4:]}</h3>")
            elif line.startswith("## "):
                if in_list: html_lines.append("</ul>"); in_list = False
                html_lines.append(f"<h3>{line[3:]}</h3>")
            elif line.startswith("# "):
                if in_list: html_lines.append("</ul>"); in_list = False
                html_lines.append(f"<h3>{line[2:]}</h3>")
            # List items
            elif line.strip().startswith("- ") or line.strip().startswith("* "):
                if not in_list:
                    html_lines.append("<ul>"); in_list = True
                item = line.strip()[2:]
                item = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", item)
                html_lines.append(f"<li>{item}</li>")
            # Empty line
            elif not line.strip():
                if in_list:
                    html_lines.append("</ul>"); in_list = False
                html_lines.append("")
            # Paragraph
            else:
                if in_list:
                    html_lines.append("</ul>"); in_list = False
                formatted = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
                html_lines.append(f"<p>{formatted}</p>")

        if in_list:
            html_lines.append("</ul>")
        return "\n".join(html_lines)
