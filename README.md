🚀 AI Operational Intelligence System
📖 Overview
An AI-powered decision engine designed to democratize access to critical operational metrics for Strategy, Planning & Analytics (SP&A) and Operations teams. Through a natural language interface, the system enables users to diagnose anomalies, track trends, and run complex benchmarks — reducing response latency from hours of manual analysis to seconds of deterministic execution.

🏗️ Architecture
The project follows a Hexagonal Architecture (Ports & Adapters), ensuring business logic remains fully independent of external tooling:

Domain Core: Business metric calculation logic, anomaly detection, and domain-specific rules.
Infrastructure Adapters:

Query Engine: DuckDB for high-performance in-memory OLAP processing.
LLM Interface: Claude 3.5 Sonnet for analytical reasoning and SQL generation.
Communication: Brevo integration for automated executive report delivery via email.


Entry Points: Dual interface via FastAPI (programmatic scalability) and Streamlit (interactive human consumption).


🧠 Agent Engine (ReAct + Self-Healing SQL)
The system implements the ReAct (Reasoning and Acting) pattern, providing full observability over the AI reasoning process:

Reasoning: The agent analyzes table schemas and a metric dictionary to determine the best query strategy.
Action: Generates and executes SQL against DuckDB.
Self-Healing: If a SQL query fails due to syntax errors or missing data, the agent captures the error, reasons about the cause, and rewrites the query automatically before responding.
Conversational Memory: Maintains state to support deep investigation threads (e.g., "And how many of those zones are from Mexico?").


🛡️ Data Quality Rules & Guardrails
To ensure reliability at operational scale:

Structural Deduplication: Cleaning layer in loader.py that eliminates redundancies in zone metadata (Country/City/Zone) using composite keys.
Proactive Data Audit: The system identifies impossible values (e.g., Lead Penetration > 100% or anomalous values in specific regions) and surfaces them as data quality findings rather than processing them blindly.
Domain Isolation: Credentials and sensitive configurations managed strictly via environment variables (.env).


🖥️ Interface Guide
The UI is divided strategically based on user profile:
1. 💬 Data Chat (Ad-hoc Analysis)

Use: Free-form questions about the dataset — trends, aggregations, comparisons.
Visualization: Dynamic interactive charts via Plotly (Line, Bar, Scatter, Heatmaps, Boxplots) for time series, distributions, and correlations.
Export: Integrated buttons to download query-specific results as CSV and PDF.

2. 📊 Insights Report (Proactive Analysis)

Use: Executive-level view of critical issues and opportunities, no prompting required.
Operational Signals: Automatic classification into 🔴 Anomalies, 🟠 Deteriorating Trends, and 🟢 Expansion Opportunities.
Distribution: Formatted PDF report sent directly to the requester's email in one click.


💰 Cost Monitoring (FinOps)
As a standard of efficiency in Generative AI projects, the interface includes a Real-Time Cost Monitor:

Estimated Cost (Claude 3.5 Sonnet): Dynamic calculation based on input/output tokens per session.
Transparency: Allows system administrators to audit the operational cost of each analytical query.

Based on testing with Claude 3.5 Sonnet, the average cost is ~$0.02 USD per complex query. A deep analysis session of 10 questions has a projected cost of $0.20 USD.

🛠️ Setup & Deployment (Docker)
The system is fully containerized for total reproducibility.
Steps
1. Configure environment variables (.env):
envANTHROPIC_API_KEY=your_anthropic_api_key
BREVO_API_KEY=your_brevo_api_key
BREVO_SENDER_EMAIL=your_verified_email@gmail.com
DATA_PATH=data/raw/operational_data.xlsx
2. Start the system:
bashdocker-compose up --build

📐 Engineering Standards

Strict Typing: Pydantic used for bidirectional schema validation.
Observability: Detailed logging of the reasoning chain (Thought/Action/Observation) in the container console.
OLAP Efficiency: DuckDB implementation avoids network latency and cloud compute costs during the exploration phase.


🔒 Security

Credential Management: API keys and secrets are managed strictly via environment variables — no sensitive information or .env files are included in version control.
Agent Guardrails: The ReAct agent is restricted to specific, predefined analytical tools, mitigating the risk of prompt injection or arbitrary code execution.
Infrastructure: Docker provides a sandboxed execution environment, ensuring system-level security and protecting the host from unauthorized access.
