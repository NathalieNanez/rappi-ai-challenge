#!/usr/bin/env python3
"""
run_local.py — Start the full stack locally without Docker.
Usage:
    python run_local.py

Requires:
    pip install -r requirements.txt
    ANTHROPIC_API_KEY set in .env or environment
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# Load .env if present
env_file = Path(".env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

# Validate API key
if not os.environ.get("ANTHROPIC_API_KEY"):
    print("ERROR: ANTHROPIC_API_KEY not set. Copy .env.example → .env and add your key.")
    sys.exit(1)

# Set default paths for local dev
os.environ.setdefault("DATA_PATH", "data/raw/rappi_data.xlsx")
os.environ.setdefault("REPORTS_PATH", "reports")
os.environ.setdefault("API_BASE_URL", "http://localhost:8000")
os.environ.setdefault("PYTHONPATH", ".")

Path("reports").mkdir(exist_ok=True)

print("🚀 Starting Rappi AI Analytics System...")
print("=" * 50)

# Start FastAPI
api_proc = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "app.api.main:app",
     "--host", "0.0.0.0", "--port", "8000", "--reload"],
    env=os.environ,
)

print("⏳ Waiting for API to start...")
time.sleep(4)

# Start Streamlit
ui_proc = subprocess.Popen(
    [sys.executable, "-m", "streamlit", "run", "app/ui/streamlit_app.py",
     "--server.port", "8501",
     "--server.address", "0.0.0.0",
     "--browser.gatherUsageStats", "false"],
    env=os.environ,
)

print("\n✅ System running!")
print("   🤖 Chat UI  → http://localhost:8501")
print("   📡 API docs → http://localhost:8000/docs")
print("   Press Ctrl+C to stop.")

try:
    api_proc.wait()
except KeyboardInterrupt:
    print("\n🛑 Stopping services...")
    api_proc.terminate()
    ui_proc.terminate()
    print("Done.")
