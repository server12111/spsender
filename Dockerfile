FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV DATA_DIR=/app/data
WORKDIR /srv/fe-autosender
COPY pyproject.toml requirements.lock README.md ./
COPY app ./app
RUN pip install --no-cache-dir -r requirements.lock \
    && pip install --no-cache-dir --no-deps . \
    && mkdir -p /app/data
COPY main.py ./
# Hosting data volumes (bothost.ru mounts /app/data) are root-owned and the
# panel offers no way to chown a volume, so the process runs as root to use
# the persistent folder. File logging degrades to console on permission errors.
# Do not declare VOLUME here: Docker can create a new anonymous volume on
# every recreated container and hide the hosting bind mount's data folder.
# Bothost provides persistence; Compose mounts the named autosender_data volume.
STOPSIGNAL SIGINT
CMD ["python", "-m", "app", "run"]
