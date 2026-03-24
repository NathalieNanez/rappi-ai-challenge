#!/bin/bash
set -e

echo "Starting Rappi AI System..."

# Start FastAPI in background
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --workers 2 &
API_PID=$!

# Give API time to start
sleep 3

# Start Streamlit
streamlit run app/ui/streamlit_app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false &
UI_PID=$!

echo "FastAPI running on :8000 (PID $API_PID)"
echo "Streamlit running on :8501 (PID $UI_PID)"

# Wait for either process to exit
wait -n $API_PID $UI_PID
