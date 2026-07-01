# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

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
    REPO_PATH=/workspace

WORKDIR /app

RUN python -m venv /opt/venv

COPY pyproject.toml AGENTS.md ./
COPY src ./src

RUN pip install --no-compile . \
    && groupadd --gid 10001 mcp \
    && useradd --uid 10001 --gid mcp --home-dir /tmp --shell /usr/sbin/nologin --no-create-home mcp \
    && mkdir -p /workspace /state \
    && chown -R mcp:mcp /workspace /state

USER mcp

EXPOSE 8000
VOLUME ["/workspace", "/state"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.getenv('PORT', '8000'), timeout=3).read()" || exit 1

CMD ["mcp-context-manager"]
