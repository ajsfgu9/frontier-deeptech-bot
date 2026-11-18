FROM python:3.11-slim

# Keep Python from writing .pyc files / buffering stdout (so `docker logs` is live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first so code edits don't bust the layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Non-root user — the whole point of container isolation is defeated if the
# process inside runs as root with a compromised process able to touch the
# rest of the filesystem it can see.
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash botuser \
    && mkdir -p /app/data \
    && chown -R botuser:botuser /app

COPY --chown=botuser:botuser *.py .

USER botuser

HEALTHCHECK --interval=5m --timeout=10s --start-period=2m --retries=3 \
    CMD test -n "$(find /app/data/heartbeat.txt -mmin -15 2>/dev/null)" || exit 1

ENTRYPOINT ["python", "-u", "app.py"]
