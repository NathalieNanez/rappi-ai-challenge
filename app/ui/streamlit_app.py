import json
import logging
import uuid
from typing import Any, Dict
import os
import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
import io
import tempfile
from fpdf import FPDF

load_dotenv()

import streamlit as st
import plotly.io as pio
import plotly.graph_objects as go

from app.bot.agent import RappiAgent, get_or_create_session, clear_session
from app.insights.engine import InsightEngine
from app.data.loader import get_loader

logger = logging.getLogger(__name__)

def generate_report_markdown(insights) -> str:
    md = "# 🚦 Reporte Ejecutivo de Inteligencia Operacional\n\n"
    md += f"**Generado por:** Rappi AI System | **Fecha:** {datetime.date.today().strftime('%Y-%m-%d')}\n\n"
    
    if not insights:
        md += "No se detectaron hallazgos en la última semana.\n"
        return md
        
    # Deduplicate insights (title and description as unique keys)
    unique_insights = {}
    for i in insights:
        key = (i.title, i.description)
        if key not in unique_insights:
            unique_insights[key] = i
        else:
            # Merge affected zones if they exist and are different
            existing = unique_insights[key]
            if i.affected_zones and existing.affected_zones:
                combined_zones = list(set(existing.affected_zones + i.affected_zones))
                existing.affected_zones = combined_zones

    deduped_list = list(unique_insights.values())

    # Count categories for the "At a Glance" section
    anomalies = [i for i in deduped_list if i.category == "anomaly"]
    trends = [i for i in deduped_list if i.category == "trend"]
    opportunities = [i for i in deduped_list if i.category in ["opportunity", "correlation", "benchmark"]]
    
    # 1. At a Glance Summary
    md += "## 📊 Resumen Ejecutivo\n\n"
    md += f"- 🔴 **Acción Urgente:** {len(anomalies)} Anomalías detectadas.\n"
    md += f"- 🟠 **Seguimiento:** {len(trends)} Tendencias de deterioro.\n"
    md += f"- 🟢 **Estrategia:** {len(opportunities)} Oportunidades y Correlaciones.\n\n"
    
    md += "---\n\n"
    
    # Helper for rendering sections
    def render_group(group_insights, icon, title):
        if not group_insights:
            return ""
        block = f"## {icon} {title}\n\n"
        for i in group_insights:
            block += f"### {i.title}\n"
            block += f"**Descripción:** {i.description}\n\n"
            block += f"> **Recomendación:** {i.recommendation}\n\n"
            if i.affected_zones:
                block += f"**Zonas Afectadas:** {', '.join(i.affected_zones)}\n\n"
        return block
        
    # 2. Logical Grouping
    md += render_group(anomalies, "🔴", "Anomalías")
    md += render_group(trends, "🟠", "Tendencias")
    md += render_group(opportunities, "🟢", "Oportunidades y Estrategia")
    
    # Footer
    md += "---\n\n"
    md += "*Este reporte fue generado automáticamente por el Rappi AI System. Por favor, no respondas a este correo.*\n"
    
    return md

def send_email_with_brevo(recipient_email: str, markdown_content: str) -> tuple[bool, str]:
    api_key = os.environ.get("BREVO_API_KEY")
    if not api_key:
        err = "BREVO_API_KEY no configurada."
        logger.error(err)
        return False, err
        
    sender_email = os.environ.get("BREVO_SENDER_EMAIL", "[TU-CORREO-DE-BREVO-AQUÍ]")
        
    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": api_key,
        "content-type": "application/json"
    }
    payload = {
        "sender": {"name": "Rappi AI System", "email": sender_email},
        "to": [{"email": recipient_email}],
        "subject": "🚦 Reporte Ejecutivo de Inteligencia Operacional - Rappi SP&A",
        "textContent": markdown_content
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            return True, "Success"
        else:
            err = f"{response.status_code} - {response.text}"
            logger.error(f"Error Brevo API: {err}")
            return False, err
    except Exception as e:
        err = str(e)
        logger.error(f"Falla de conexión con Brevo: {err}")
        return False, err

def export_chat_to_pdf(question: str, answer: str, csv_data: str, charts_json: list[str] = None) -> bytes:
    pdf = FPDF()
    pdf.set_margins(left=15, top=15, right=15)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    
    w = pdf.w - 2 * pdf.l_margin
    
    pdf.set_font("helvetica", "B", 16)
    pdf.set_text_color(255, 68, 31)
    pdf.multi_cell(w, 8, "Rappi Data Query Export", align="C")
    pdf.ln(5)
    
    pdf.set_font("helvetica", "B", 12)
    pdf.set_text_color(0, 0, 0)
    pdf.multi_cell(w, 7, f"Pregunta: {question}".encode('latin-1', 'replace').decode('latin-1'))
    pdf.ln(3)
    
    pdf.set_font("helvetica", "", 11)
    pdf.multi_cell(w, 6, f"Respuesta:\n{answer}".encode('latin-1', 'replace').decode('latin-1'))
    pdf.ln(5)
    
    if charts_json:
        pdf.set_font("helvetica", "B", 11)
        pdf.multi_cell(w, 7, "Visualización(es):")
        pdf.ln(2)
        for chart_str in charts_json:
            try:
                fig = pio.from_json(chart_str)
                fig.update_layout(paper_bgcolor='white', plot_bgcolor='white')
                # Use a temporary file to guarantee compatibility with all FPDF2 versions
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    fig.write_image(tmp.name, format="png", width=700, height=400)
                    pdf.image(tmp.name, w=w)
                os.remove(tmp.name)
                pdf.ln(5)
            except Exception as e:
                pdf.multi_cell(w, 5, f"[Error al adjuntar gráfica: {e}]")
                pdf.ln(2)
                
    if csv_data:
        pdf.set_font("helvetica", "B", 11)
        pdf.multi_cell(w, 7, "Datos Adjuntos:")
        pdf.set_font("courier", "", 9)
        try:
            df = pd.read_csv(io.StringIO(csv_data))
            text_data = df.head(40).to_string(index=False)
            pdf.multi_cell(w, 5, text_data.encode('latin-1', 'replace').decode('latin-1'))
            if len(df) > 40:
                pdf.ln(2)
                pdf.set_font("helvetica", "I", 10)
                pdf.multi_cell(w, 5, f"... ({len(df)-40} filas omitidas)")
        except Exception as e:
            pdf.multi_cell(w, 5, f"No se pudo renderizar la tabla: {e}")
            
    return bytes(pdf.output())

# ------------------------------------------------------------------------------
# 1. Page Config & Brand Styling
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Rappi Data Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject custom CSS for Rappi branding
st.markdown(
    """
    <style>
    /* Rappi Primary Brand Color: #FF441F */
    :root {
        --primary-color: #FF441F;
    }
    .stButton>button {
        color: white;
        background-color: var(--primary-color);
        border: none;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #E03D1A;
        color: white;
    }
    h1, h2, h3 {
        color: var(--primary-color);
    }
    .sidebar-content {
        padding: 1rem;
        background-color: #f8f9fa;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------------------------------
# 2. Session Initialization
# ------------------------------------------------------------------------------
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

if "prompt_cache" not in st.session_state:
    st.session_state.prompt_cache = None

if "agent" not in st.session_state:
    st.session_state.agent = RappiAgent()

if "loader" not in st.session_state:
    st.session_state.loader = get_loader()

if "engine" not in st.session_state:
    st.session_state.engine = InsightEngine()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "welcomed" not in st.session_state:
    st.session_state.welcomed = False

if "cost_stats" not in st.session_state:
    st.session_state.cost_stats = {
        "total_calls": 0,
        "total_tokens": 0,
        "estimated_cost_usd": 0.0,
    }

session = get_or_create_session(st.session_state.session_id)

# ------------------------------------------------------------------------------
# 3. Sidebar (Cost Monitor & Actions)
# ------------------------------------------------------------------------------
with st.sidebar:
    st.image("app/ui/Rappi_logo.png", width=100)
    st.markdown("### Asistente de Datos SP&A")
    st.markdown("---")
    
    # Cost Monitor Section
    st.markdown("#### Monitor de Costos API")
    stats = st.session_state.cost_stats
    
    col1, col2 = st.columns(2)
    col1.metric("Consultas a la API", stats["total_calls"])
    col2.metric("Consumo de IA", f"{stats['total_tokens']:,}")
    st.metric("Inversión Total (USD)", f"${stats['estimated_cost_usd']:.4f}")
    
    st.markdown("---")
    
    # Clear History Action
    if st.button("Limpiar Historial", use_container_width=True):
        clear_session(st.session_state.session_id)
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.chat_history = []
        st.session_state.welcomed = False
        st.session_state.proactive_insights = None
        st.session_state.cost_stats = {
            "total_calls": 0, "total_tokens": 0, "estimated_cost_usd": 0.0
        }
        st.rerun()

# ------------------------------------------------------------------------------
# 4. Main Interface Tabs
# ------------------------------------------------------------------------------
st.title("Rappi Data Intelligence")
st.markdown("Haz preguntas en lenguaje natural sobre las métricas operacionales.")

if "proactive_insights" not in st.session_state:
    st.session_state.proactive_insights = None

tab_chat, tab_report = st.tabs(["💬 Chat de Datos", "📋 Reporte de Insights"])

# ------------------------------------------------------------------------------
# Tab 2: Reporte Ejecutivo (Proactive Insights)
# ------------------------------------------------------------------------------
with tab_report:
    st.markdown("### Principales hallazgos críticos")
    if st.session_state.proactive_insights is None:
        with st.spinner("Analizando métricas y detectando anomalías..."):
            try:
                st.session_state.proactive_insights = st.session_state.engine.run(max_insights_per_category=2)
                st.success("Reporte actualizado")
            except Exception as e:
                st.error(f"Error generando reporte: {e}")
                st.session_state.proactive_insights = []

    # Botón de Descarga del Reporte Completo
    if st.session_state.proactive_insights is not None:
        report_md = generate_report_markdown(st.session_state.proactive_insights)
        st.download_button(
            label="📥 Descargar Reporte Ejecutivo (.md)",
            data=report_md.encode('utf-8'),
            file_name="reporte_ejecutivo_rappi.md",
            mime="text/markdown",
            help="Descarga un resumen consolidado de todos los hallazgos operativos en formato Markdown."
        )
        
        # --- Email Integration UI ---
        st.markdown("<br>", unsafe_allow_html=True)
        recipient_email = st.text_input("📧 Enviar Reporte por Correo", placeholder="ejemplo@rappi.com")
        if st.button("📧 Enviar al Correo"):
            if recipient_email.strip():
                with st.spinner("Enviando correo..."):
                    success, error_msg = send_email_with_brevo(recipient_email.strip(), report_md)
                    if success:
                        st.success(f"¡Reporte enviado con éxito a {recipient_email.strip()}!")
                    else:
                        st.error(f"Error al enviar el correo: {error_msg}")
            else:
                st.warning("Por favor ingresa un correo electrónico.")
                
        st.markdown("---")
        st.markdown(report_md)

# ------------------------------------------------------------------------------
# Tab 1: Chat de Datos
# ------------------------------------------------------------------------------
with tab_chat:
    # Render existing chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            
            # Render charts if present
            if msg.get("charts"):
                for chart_json in msg["charts"]:
                    try:
                        fig = pio.from_json(chart_json)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error al renderizar gráfico: {e}")

            # Render downloaded CSVs or PDF if chart/data is present
            has_data = bool(msg.get("csv_buffers"))
            has_charts = bool(msg.get("charts"))
            
            if has_data or has_charts:
                q = msg.get("question", "Consulta no especificada")
                ans = msg.get("content", "")
                
                # Render at least 1 toolbar
                toolbar_count = max(1, len(msg.get("csv_buffers", [])))
                
                for idx in range(toolbar_count):
                    c1, c2, c3 = st.columns([1, 1, 3])
                    csv_str = msg["csv_buffers"][idx] if has_data and idx < len(msg["csv_buffers"]) else ""
                    
                    with c1:
                        if csv_str:
                            st.download_button(
                                label="📥 Descargar Datos (CSV)",
                                data=csv_str,
                                file_name=f"analisis_rappi_chat_{idx}.csv",
                                mime="text/csv",
                                key=f"dl_hist_{st.session_state.chat_history.index(msg)}_{idx}"
                            )
                        else:
                            st.button("📥 Descargar Datos (CSV)", key=f"dl_hist_dis_{st.session_state.chat_history.index(msg)}", disabled=True, help="No hay tabla de datos en esta respuesta.")
                            
                    with c2:
                        pdf_bytes = export_chat_to_pdf(q, ans, csv_str, msg.get("charts", []))
                        st.download_button(
                            label="📄 Descargar Respuesta (PDF)",
                            data=pdf_bytes,
                            file_name=f"respuesta_rappi_chat_{idx}.pdf",
                            mime="application/pdf",
                            key=f"dl_pdf_hist_{st.session_state.chat_history.index(msg)}_{idx}"
                        )

            # Render followups
            if msg.get("followups"):
                st.markdown("**Preguntas sugeridas:**")
                for f_idx, f in enumerate(msg["followups"]):
                    if st.button(f, key=f"hist_btn_{st.session_state.chat_history.index(msg)}_{f_idx}"):
                        st.session_state.prompt_cache = f

    # Evaluate prompt click from buttons
    prompt = st.chat_input("Ej: ¿Cuál es la tendencia de Perfect Orders en Bogotá?")
    if st.session_state.prompt_cache:
        prompt = st.session_state.prompt_cache
        st.session_state.prompt_cache = None

    # User Input & Streaming Response
    if prompt:
        st.session_state.chat_history.append({
            "role": "user", 
            "content": prompt, 
            "charts": [], 
            "csv_buffers": [],
            "followups": []
        })
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            charts = []
            csv_buffers = []
            followups = []
            
            try:
                stream = st.session_state.agent.stream_chat(session, prompt)
                for event in stream:
                    if event["type"] == "text_delta":
                        full_response += event["delta"]
                        message_placeholder.markdown(full_response + "▌")
                    
                    elif event["type"] == "chart":
                        charts.append(event["figure_json"])
                    
                    elif event["type"] == "csv_data":
                        csv_buffers.append(event["csv_content"])
                    
                    elif event["type"] == "followups":
                        followups = event["questions"]
                    
                    elif event["type"] == "cost":
                        st.session_state.cost_stats = event["cost_report"]

                message_placeholder.markdown(full_response)
                
                for chart_json in charts:
                    try:
                        fig = pio.from_json(chart_json)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error de renderizado gráfico: {e}")
                
                has_data = bool(csv_buffers)
                has_charts = bool(charts)
                
                if has_data or has_charts:
                    toolbar_count = max(1, len(csv_buffers))
                    for idx in range(toolbar_count):
                        c1, c2, c3 = st.columns([1, 1, 3])
                        csv_str = csv_buffers[idx] if has_data and idx < len(csv_buffers) else ""
                        
                        with c1:
                            if csv_str:
                                st.download_button(
                                    label="📥 Descargar Datos (CSV)",
                                    data=csv_str,
                                    file_name=f"analisis_rappi_chat_{idx}.csv",
                                    mime="text/csv",
                                    key=f"dl_stream_{idx}"
                                )
                            else:
                                st.button("📥 Descargar Datos (CSV)", key=f"dl_stream_dis_{idx}", disabled=True, help="No hay tabla de datos en esta respuesta.")
                                
                        with c2:
                            pdf_bytes = export_chat_to_pdf(prompt, full_response, csv_str, charts)
                            st.download_button(
                                label="📄 Descargar Respuesta (PDF)",
                                data=pdf_bytes,
                                file_name=f"respuesta_rappi_chat_{idx}.pdf",
                                mime="application/pdf",
                                key=f"dl_pdf_stream_{idx}"
                            )
                
                if followups:
                    st.markdown("**Preguntas sugeridas:**")
                    for f_idx, f in enumerate(followups):
                        if st.button(f, key=f"stream_btn_{f_idx}"):
                            st.session_state.prompt_cache = f
                            st.rerun()

                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": full_response,
                    "question": prompt,
                    "charts": charts,
                    "csv_buffers": csv_buffers,
                    "followups": followups,
                })
                
                st.rerun()

            except Exception as e:
                st.error(f"Error procesando la solicitud: {e}")
