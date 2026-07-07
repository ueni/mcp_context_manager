# syntax=docker/dockerfile:1.7
FROM python:3.12-alpine AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_NO_COMPILE=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apk add --no-cache build-base git libffi-dev openssl-dev \
    && python -m venv /opt/venv

COPY pyproject.toml README.md ./
COPY src ./src

RUN /opt/venv/bin/python -m pip install --upgrade pip \
    && /opt/venv/bin/python -m pip install .

FROM python:3.12-alpine AS runtime

ARG MCP_CONTEXT_UID=1000
ARG MCP_CONTEXT_GID=1000

ENV HOME=/tmp \
    HOST=0.0.0.0 \
    MCP_CONTEXT_STATE_DIR=/state \
    MCP_TRANSPORT=streamable-http \
    PATH=/opt/venv/bin:$PATH \
    PORT=8000 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REPO_PATH=/workspace-roots

WORKDIR /app

RUN apk add --no-cache ca-certificates git \
    && addgroup -S -g "${MCP_CONTEXT_GID}" mcp \
    && adduser -S -D -H -h /tmp -s /sbin/nologin -u "${MCP_CONTEXT_UID}" -G mcp mcp \
    && mkdir -p /workspace-roots /state \
    && chown -R mcp:mcp /workspace-roots /state

COPY --from=builder /opt/venv /opt/venv

USER mcp

EXPOSE 8000
VOLUME ["/workspace-roots", "/state"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.getenv('PORT', '8000'), timeout=3).read()" || exit 1

CMD ["mcp-context-manager"]
