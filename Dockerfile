# syntax=docker/dockerfile:1.7
FROM alpine:3.21 AS verify

ARG SERVER_BINARY=dist/mcp-context-manager-linux-x86_64-musl

RUN apk add --no-cache binutils file
COPY ${SERVER_BINARY} /mcp-context-manager
RUN chmod 0755 /mcp-context-manager \
    && file /mcp-context-manager | grep -Eq 'ELF 64-bit.*static' \
    && ! readelf -l /mcp-context-manager | grep -q 'INTERP' \
    && /mcp-context-manager --version | grep -q '^mcp-context-manager '

FROM alpine:3.21

ARG MCP_CONTEXT_UID=1000
ARG MCP_CONTEXT_GID=1000
ENV HOME=/tmp \
    HOST=0.0.0.0 \
    MCP_CONTEXT_STATE_DIR=/state \
    MCP_TRANSPORT=streamable-http \
    PORT=8000 \
    REPO_PATH=/workspace-roots

WORKDIR /app

RUN apk add --no-cache ca-certificates curl zlib \
    && addgroup -S -g "${MCP_CONTEXT_GID}" mcp \
    && adduser -S -D -H -h /tmp -s /sbin/nologin -u "${MCP_CONTEXT_UID}" -G mcp mcp \
    && mkdir -p /workspace-roots /state \
    && chown -R mcp:mcp /workspace-roots /state

COPY --from=verify /mcp-context-manager /usr/local/bin/mcp-context-manager
RUN chmod +x /usr/local/bin/mcp-context-manager \
    && chown mcp:mcp /usr/local/bin/mcp-context-manager

USER mcp

EXPOSE 8000
VOLUME ["/workspace-roots", "/state"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "curl -fsS \"http://127.0.0.1:${PORT}/healthz\" | grep -q '\"ok\":true' || exit 1"]

ENTRYPOINT ["/usr/local/bin/mcp-context-manager"]
