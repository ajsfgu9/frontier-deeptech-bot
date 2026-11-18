FROM python:3.11-slim

# Keep Python from writing .pyc files / buffering stdout (so `docker logs` is live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first so code edits don't bust the layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Non-root user + gosu so the entrypoint can fix ./data bind-mount ownership
# before dropping privileges to botuser (UID 1000).
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 gosu && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home --shell /bin/bash botuser \
    && mkdir -p /app/data \
    && chown -R botuser:botuser /app

COPY --chown=botuser:botuser *.py .

HEALTHCHECK --interval=5m --timeout=10s --start-period=2m --retries=3 \
    CMD test -n "$(find /app/data/heartbeat.txt /tmp/frontier_deeptech_data/heartbeat.txt -mmin -15 2>/dev/null)" || exit 1

ENTRYPOINT ["sh", "-c", "mkdir -p /app/data && chown -R botuser:botuser /app/data 2>/dev/null || true; exec gosu botuser python -u app.py"]
