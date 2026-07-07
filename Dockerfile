# syntax=docker/dockerfile:1.7
FROM alpine:3.21

ARG MCP_CONTEXT_UID=1000
ARG MCP_CONTEXT_GID=1000
ARG SERVER_BINARY=dist/mcp-context-manager

ENV HOME=/tmp \
    HOST=0.0.0.0 \
    MCP_CONTEXT_STATE_DIR=/state \
    MCP_TRANSPORT=streamable-http \
    PORT=8000 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REPO_PATH=/workspace-roots

WORKDIR /app

RUN apk add --no-cache ca-certificates curl gcompat \
    && addgroup -S -g "${MCP_CONTEXT_GID}" mcp \
    && adduser -S -D -H -h /tmp -s /sbin/nologin -u "${MCP_CONTEXT_UID}" -G mcp mcp \
    && mkdir -p /workspace-roots /state \
    && chown -R mcp:mcp /workspace-roots /state

COPY ${SERVER_BINARY} /usr/local/bin/mcp-context-manager
RUN chmod +x /usr/local/bin/mcp-context-manager \
    && chown mcp:mcp /usr/local/bin/mcp-context-manager

USER mcp

EXPOSE 8000
VOLUME ["/workspace-roots", "/state"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "curl -fsS \"http://127.0.0.1:${PORT}/healthz\" | grep -q '\"ok\":true' || exit 1"]

ENTRYPOINT ["/usr/local/bin/mcp-context-manager"]
