#!/usr/bin/env bash
set -euo pipefail

repo="${MCP_CONTEXT_UPDATE_REPO:-ueni/mcp_context_manager}"
root="${MCP_CONTEXT_UPDATE_ROOT:-/app}"
state_dir="${MCP_CONTEXT_UPDATE_STATE_DIR:-/state/release-update}"
paths="${MCP_CONTEXT_UPDATE_PATHS:-pyproject.toml README.md scripts src}"
interval="${MCP_CONTEXT_UPDATE_INTERVAL_SECONDS:-3600}"

latest_url() {
  local target="${1:-latest}"
  if [[ "$target" == http://* || "$target" == https://* ]]; then
    printf '%s\n' "$target"
    return
  fi
  if [[ "$target" == "latest" ]]; then
    target="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${repo}/releases/latest")"
    target="${target##*/}"
  elif [[ "$target" =~ ^[0-9] ]]; then
    target="v${target}"
  fi
  printf 'https://github.com/%s/archive/refs/tags/%s.tar.gz\n' "$repo" "$target"
}

copy_path() {
  local src="$1/$2"
  local dst="$root/$2"
  [[ -e "$src" ]] || return 0
  if [[ -d "$src" ]]; then
    mkdir -p "$dst"
    find "$dst" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    cp -a "$src/." "$dst/"
    return
  fi
  mkdir -p "$(dirname "$dst")"
  cp -a "$src" "$dst"
}

update_once() {
  local url archive work source
  url="$(latest_url "${1:-latest}")"
  mkdir -p "$state_dir"
  if [[ -f "$state_dir/url" ]] && [[ "$(cat "$state_dir/url")" == "$url" ]]; then
    echo "No new release: $url"
    return 0
  fi

  work="$(mktemp -d)"
  archive="$work/source.tar.gz"
  curl -fL "$url" -o "$archive"
  tar -xzf "$archive" -C "$work"
  source="$(find "$work" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "$source" ]] || { echo "Archive has no source directory" >&2; exit 1; }

  for path in $paths; do
    copy_path "$source" "$path"
  done
  rm -rf "$work"

  (cd "$root" && python -m pip install --no-compile .)
  printf '%s\n' "$url" > "$state_dir/url"
  echo "Updated from $url"
  kill -TERM "${MCP_CONTEXT_RESTART_PID:-1}"
}

watch_updates() {
  while true; do
    update_once latest || echo "Release update failed; retrying in ${interval}s" >&2
    sleep "$interval"
  done
}

serve() {
  case "${MCP_CONTEXT_AUTO_UPDATE:-1}" in
    1|true|TRUE|yes|YES|on|ON)
      watch_updates &
      ;;
  esac
  exec mcp-context-manager
}

case "${1:-latest}" in
  serve)
    serve
    ;;
  --watch)
    watch_updates
    ;;
  *)
    target="${1:-latest}"
    if [[ "${2:-}" == "--watch" ]]; then
      watch_updates
    else
      update_once "$target"
    fi
    ;;
esac
