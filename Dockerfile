FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv/fe-autosender
COPY pyproject.toml requirements.lock README.md ./
COPY app ./app
RUN pip install --no-cache-dir -r requirements.lock \
    && pip install --no-cache-dir --no-deps . \
    && useradd --uid 10001 --create-home autosender \
    && mkdir -p /srv/fe-autosender/data \
    && chown autosender:autosender /srv/fe-autosender/data
COPY main.py ./
USER 10001:10001
STOPSIGNAL SIGINT
CMD ["python", "-m", "app", "run"]
