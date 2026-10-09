FROM ghcr.io/astral-sh/uv:0.12.23 AS uv
FROM python:3.14.8-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PYTHON_PREFERENCE=only-managed \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PSYCOPG_IMPL=python \
    DATAFLOW_DATA_ROOT=/dataflow \
    DATAFLOW_WORK_ROOT=/work
RUN uv python install 3.14.8t && uv venv --python 3.14.8t /opt/venv
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN uv pip install --python /opt/venv/bin/python '.[postgres]' \
    && /opt/venv/bin/python -c 'import psycopg; import sys; assert not sys._is_gil_enabled()' \
    && dataflow doctor \
    && mkdir -p /work /dataflow && chown -R 10001:10001 /work /dataflow
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["dataflow"]
CMD ["control", "--host", "0.0.0.0", "--data-root", "/dataflow"]
