FROM ghcr.io/astral-sh/uv:0.12.23 AS uv
FROM python:3.14.8-slim-bookworm
ARG PYTHON_VERSION=3.14.8t
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
RUN uv python install "$PYTHON_VERSION" && uv venv --python "$PYTHON_VERSION" /opt/venv
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY constraints ./constraints
RUN uv pip install --python /opt/venv/bin/python -c constraints/production.txt '.[postgres]' \
    && /opt/venv/bin/python -c 'import psycopg, sysconfig; from dataflowcore.runtime import check_runtime; check_runtime(allow_gil=not sysconfig.get_config_var("Py_GIL_DISABLED"))' \
    && dataflow doctor \
    && mkdir -p /work /dataflow && chown -R 10001:10001 /work /dataflow
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["dataflow"]
CMD ["control", "--host", "0.0.0.0", "--data-root", "/dataflow"]
