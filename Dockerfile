# ------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------
FROM python:3.10-slim AS builder

WORKDIR /app

# Install build dependencies if necessary
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies to the local user directory
RUN pip install --user --no-cache-dir -r requirements.txt

# ------------------------------------------------------------------------------
# Stage 2: Final Runtime
# ------------------------------------------------------------------------------
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies required by Kaleido for Plotly image export
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libasound2 \
    libxss1 \
    libgtk-3-0 \
    libgbm-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy installed dependencies from the builder stage
COPY --from=builder /root/.local /root/.local

# Ensure the local bin is on the PATH
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app

# Copy application source code
COPY . .

# Expose ports for FastAPI (8000) and Streamlit (8501)
EXPOSE 8000 8501

# Run both the FastAPI backend and Streamlit frontend concurrently
CMD ["sh", "-c", "uvicorn app.api.main:app --host 0.0.0.0 --port 8000 & streamlit run app/ui/streamlit_app.py --server.port 8501 --server.address 0.0.0.0"]
