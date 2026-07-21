#!/usr/bin/env bash

set -u

readonly MCP_SERVICE_LABEL="com.docker.compose.service=mcp-context-manager"
readonly MCP_SERVICE_NAME="mcp-context-manager"

warn() {
  echo "devcontainer MCP network: $*" >&2
}

docker_bin="${DOCKER_BIN:-}"
if [ -z "$docker_bin" ]; then
  docker_bin="$(command -v docker 2>/dev/null || true)"
fi
if [ -z "$docker_bin" ] && [ -x "$HOME/.local/bin/docker" ]; then
  docker_bin="$HOME/.local/bin/docker"
fi

if [ -z "$docker_bin" ]; then
  warn "Docker CLI is unavailable; continuing without the sibling MCP network"
  exit 0
fi

run_docker() {
  if "$docker_bin" "$@" 2>/dev/null; then
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n "$docker_bin" "$@" 2>/dev/null; then
    return 0
  fi
  return 1
}

sibling_id="$(run_docker ps --filter "label=$MCP_SERVICE_LABEL" --format '{{.ID}}' | sed -n '1p')"
if [ -z "$sibling_id" ]; then
  warn "no running $MCP_SERVICE_NAME sibling found; continuing without its network"
  exit 0
fi

sibling_network=""
while read -r network alias; do
  if [ "$alias" = "$MCP_SERVICE_NAME" ]; then
    sibling_network="$network"
    break
  fi
done < <(run_docker inspect --format '{{range $name, $network := .NetworkSettings.Networks}}{{range $alias := $network.Aliases}}{{println $name $alias}}{{end}}{{end}}' "$sibling_id")
if [ -z "$sibling_network" ]; then
  warn "no sibling network has the $MCP_SERVICE_NAME DNS alias; continuing"
  exit 0
fi

if ! run_docker network inspect "$sibling_network" >/dev/null; then
  warn "the sibling MCP network is unavailable; continuing without it"
  exit 0
fi

current_ref="${DEVCONTAINER_ID:-${HOSTNAME:-}}"
if [ -z "$current_ref" ]; then
  current_ref="$(hostname 2>/dev/null || true)"
fi
if [ -z "$current_ref" ]; then
  warn "unable to identify the current devcontainer; continuing"
  exit 0
fi

current_id="$(run_docker inspect --format '{{.Id}}' "$current_ref")"
if [ -z "$current_id" ]; then
  warn "unable to inspect the current devcontainer; continuing"
  exit 0
fi

current_networks="$(run_docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$current_id")"
if printf '%s\n' "$current_networks" | grep -Fxq "$sibling_network"; then
  exit 0
fi

if run_docker network connect "$sibling_network" "$current_id"; then
  exit 0
fi

warn "could not connect the devcontainer to the sibling MCP network; continuing"
exit 0
