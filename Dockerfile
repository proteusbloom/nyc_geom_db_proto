# syntax=docker/dockerfile:4.6
FROM python:3.11-slim

# ---- env + defaults ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ---- system deps (keep minimal; add as needed) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---- install python deps with build cache ----
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# ---- copy project code ----
COPY . .

# ---- runtime user + dirs ----
RUN useradd -m appuser \
    && mkdir -p /output /data \
    && chown -R appuser:appuser /app /output /data

USER appuser

# Optional: declare mount points
VOLUME ["/output", "/data"]

# If agent.py is the orchestrator:
ENTRYPOINT ["python"]
CMD ["-m", "agent"]

