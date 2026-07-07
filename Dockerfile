# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base

ARG MCP_CONTEXT_UID=1000
ARG MCP_CONTEXT_GID=1000

ENV HOME=/tmp \
    HOST=0.0.0.0 \
    MCP_CONTEXT_STATE_DIR=/state \
    MCP_TRANSPORT=streamable-http \
    PATH=/opt/venv/bin:$PATH \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_COMPILE=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REPO_PATH=/workspace-roots

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl git tar \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv /opt/venv \
    && groupadd --gid "${MCP_CONTEXT_GID}" mcp \
    && useradd --uid "${MCP_CONTEXT_UID}" --gid mcp --home-dir /tmp --shell /usr/sbin/nologin --no-create-home mcp \
    && mkdir -p /workspace-roots /state \
    && chown -R mcp:mcp /workspace-roots /state

COPY pyproject.toml README.md ./
COPY scripts ./scripts
COPY src ./src
RUN chmod +x scripts/*.sh

FROM base AS test

COPY benchmarks ./benchmarks
COPY monitor-metrics.py ./
COPY tests ./tests

RUN pip install --no-compile ".[dev]"

ENV PYTEST_ADDOPTS="-p no:cacheprovider" \
    RUFF_CACHE_DIR=/tmp/ruff-cache

USER mcp

CMD ["python", "-m", "pytest"]

FROM base AS runtime

RUN pip install --no-compile . \
    && chown -R mcp:mcp /app /opt/venv

USER mcp

EXPOSE 8000
VOLUME ["/workspace-roots", "/state"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.getenv('PORT', '8000'), timeout=3).read()" || exit 1

CMD ["scripts/update-from-release.sh", "serve"]
