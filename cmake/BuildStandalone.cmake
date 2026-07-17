cmake_minimum_required(VERSION 3.20)

include("${CMAKE_CURRENT_LIST_DIR}/DownloadCache.cmake")

foreach(required_var IN ITEMS
        MCP_SOURCE_DIR
        MCP_DIST_DIR
        MCP_DOWNLOAD_CACHE_DIR
        MCP_DOCKER_WORKSPACE_DIR
        MCP_DOCKER_DOWNLOAD_CACHE_DIR
        MCP_RUNTIME
        MCP_CONTAINER_IMAGE
        MCP_OUTPUT_NAME)
    if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
        message(FATAL_ERROR "${required_var} is required")
    endif()
endforeach()

mcp_prepare_download_cache()
mcp_docker_command(MCP_DOCKER_COMMAND)

set(_common_mounts
    -v "${MCP_DOCKER_WORKSPACE_DIR}:/workspace"
    -v "${MCP_DOCKER_DOWNLOAD_CACHE_DIR}:/workspace/.downloads"
    -v "/etc/ssl/certs:/host-ssl-certs:ro"
    -w /workspace
)

if(MCP_RUNTIME STREQUAL "glibc")
    set(_build_script [=[
set -eu
mkdir -p /workspace/dist /workspace/.downloads/apt/cache/partial \
  /workspace/.downloads/apt/lists/partial /workspace/.downloads/cargo \
  /workspace/.downloads/cargo-target/glibc
rm -f /etc/apt/apt.conf.d/docker-clean
APT_CACHE_OPTIONS="-o Dir::Cache::archives=/workspace/.downloads/apt/cache -o Dir::State::lists=/workspace/.downloads/apt/lists"
apt-get $APT_CACHE_OPTIONS update
apt-get $APT_CACHE_OPTIONS install -y --no-install-recommends \
  ca-certificates binutils file
update-ca-certificates
CERT_FILE=/etc/ssl/certs/ca-certificates.crt
if [ -f /host-ssl-certs/ca-certificates.crt ]; then
  CERT_FILE=/host-ssl-certs/ca-certificates.crt
fi
export SSL_CERT_FILE="$CERT_FILE"
export CARGO_HTTP_CAINFO="$CERT_FILE"
export CARGO_HOME=/workspace/.downloads/cargo
export CARGO_TARGET_DIR=/workspace/.downloads/cargo-target/glibc
TARGET=x86_64-unknown-linux-gnu
rustup target add "$TARGET"
cargo build --locked --release --target "$TARGET" -p contextd --bin mcp-context-manager
OUTPUT="/workspace/dist/$MCP_OUTPUT_NAME"
cp "$CARGO_TARGET_DIR/$TARGET/release/mcp-context-manager" "$OUTPUT"
chmod 0755 "$OUTPUT"
file "$OUTPUT" | grep -Eq 'ELF 64-bit.*dynamically linked'
readelf -l "$OUTPUT" | grep -q 'Requesting program interpreter: /lib64/ld-linux-x86-64.so.2'
! ldd "$OUTPUT" | grep -q 'not found'
"$OUTPUT" --version | grep -q '^mcp-context-manager '
chown "$(stat -c "%u:%g" /workspace)" "$OUTPUT"
]=])
elseif(MCP_RUNTIME STREQUAL "musl")
    set(_build_script [=[
set -eu
mkdir -p /workspace/dist /workspace/.downloads/apk \
  /workspace/.downloads/cargo /workspace/.downloads/cargo-target/musl
apk add --cache-dir /workspace/.downloads/apk --update-cache \
  ca-certificates binutils file musl-dev
update-ca-certificates
CERT_FILE=/etc/ssl/certs/ca-certificates.crt
if [ -f /host-ssl-certs/ca-certificates.crt ]; then
  CERT_FILE=/host-ssl-certs/ca-certificates.crt
fi
export SSL_CERT_FILE="$CERT_FILE"
export CARGO_HTTP_CAINFO="$CERT_FILE"
export CARGO_HOME=/workspace/.downloads/cargo
export CARGO_TARGET_DIR=/workspace/.downloads/cargo-target/musl
TARGET=x86_64-unknown-linux-musl
rustup target add "$TARGET"
cargo build --locked --release --target "$TARGET" -p contextd --bin mcp-context-manager
OUTPUT="/workspace/dist/$MCP_OUTPUT_NAME"
cp "$CARGO_TARGET_DIR/$TARGET/release/mcp-context-manager" "$OUTPUT"
chmod 0755 "$OUTPUT"
file "$OUTPUT" | grep -Eq 'ELF 64-bit.*static'
if readelf -l "$OUTPUT" | grep -q 'INTERP'; then
  echo "musl artifact unexpectedly contains a dynamic interpreter" >&2
  exit 1
fi
"$OUTPUT" --version | grep -q '^mcp-context-manager '
chown "$(stat -c "%u:%g" /workspace)" "$OUTPUT"
]=])
else()
    message(FATAL_ERROR "unsupported MCP_RUNTIME: ${MCP_RUNTIME}")
endif()

execute_process(
    COMMAND ${MCP_DOCKER_COMMAND} run --rm
        ${_common_mounts}
        -e "MCP_OUTPUT_NAME=${MCP_OUTPUT_NAME}"
        "${MCP_CONTAINER_IMAGE}"
        sh -ec "${_build_script}"
    WORKING_DIRECTORY "${MCP_SOURCE_DIR}"
    RESULT_VARIABLE _result
)

if(NOT _result EQUAL 0)
    message(FATAL_ERROR "failed to build ${MCP_OUTPUT_NAME}")
endif()

if(NOT EXISTS "${MCP_DIST_DIR}/${MCP_OUTPUT_NAME}")
    message(FATAL_ERROR "expected output was not produced: ${MCP_DIST_DIR}/${MCP_OUTPUT_NAME}")
endif()
