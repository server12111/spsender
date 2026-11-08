FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv/fe-autosender
COPY pyproject.toml requirements.lock README.md ./
COPY app ./app
RUN pip install --no-cache-dir -r requirements.lock \
    && pip install --no-cache-dir --no-deps . \
    && mkdir -p /srv/fe-autosender/data
COPY main.py ./
# Hosting data volumes (bothost.ru mounts /app/data) are root-owned and the
# panel offers no way to chown a volume, so the process runs as root to use
# the persistent folder. File logging degrades to console on permission errors.
# Declare the persistent data mount point so platforms create a volume even
# without an explicit mount: everything durable (SQLite DB, media, logs)
# lives under /app/data via DATABASE_PATH.
VOLUME ["/app/data"]
STOPSIGNAL SIGINT
CMD ["python", "-m", "app", "run"]
