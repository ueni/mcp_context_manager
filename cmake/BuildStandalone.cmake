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
    -w /workspace
)

if(MCP_RUNTIME STREQUAL "glibc")
    set(_build_script [=[
set -eu
mkdir -p /workspace/dist \
  /workspace/.downloads/apt/cache/partial \
  /workspace/.downloads/apt/lists/partial \
  /workspace/.downloads/pip \
  /workspace/.downloads/pyinstaller
chown -R 0:0 /workspace/.downloads/pip /workspace/.downloads/pyinstaller
rm -f /etc/apt/apt.conf.d/docker-clean
APT_CACHE_OPTIONS="-o Dir::Cache::archives=/workspace/.downloads/apt/cache -o Dir::State::lists=/workspace/.downloads/apt/lists"
apt-get $APT_CACHE_OPTIONS update
apt-get $APT_CACHE_OPTIONS install -y --no-install-recommends \
  python3 python3-pip python3.10-dev libpython3.10 \
  libffi-dev libssl-dev binutils build-essential
export PIP_CACHE_DIR=/workspace/.downloads/pip
export PIP_ROOT_USER_ACTION=ignore
python3 -m pip install --upgrade pip
python3 -m pip install pyinstaller .
pyinstaller \
  --onefile \
  --optimize 1 \
  --name "$MCP_OUTPUT_NAME" \
  --distpath /workspace/dist \
  --workpath "/workspace/.downloads/pyinstaller/build-$MCP_OUTPUT_NAME" \
  --specpath "/workspace/.downloads/pyinstaller/spec-$MCP_OUTPUT_NAME" \
  --collect-all mcp_context_manager \
  src/mcp_context_manager/__main__.py
chown -R "$(stat -c "%u:%g" /workspace)" /workspace/dist /workspace/.downloads
]=])
elseif(MCP_RUNTIME STREQUAL "musl")
    set(_build_script [=[
set -eu
mkdir -p /workspace/dist \
  /workspace/.downloads/apk \
  /workspace/.downloads/pip \
  /workspace/.downloads/pyinstaller
chown -R 0:0 /workspace/.downloads/pip /workspace/.downloads/pyinstaller
apk add --cache-dir /workspace/.downloads/apk --update-cache \
  binutils build-base musl-dev libffi-dev openssl-dev
export PIP_CACHE_DIR=/workspace/.downloads/pip
export PIP_ROOT_USER_ACTION=ignore
python -m pip install --upgrade pip
python -m pip install pyinstaller .
pyinstaller \
  --onefile \
  --optimize 1 \
  --name "$MCP_OUTPUT_NAME" \
  --distpath /workspace/dist \
  --workpath "/workspace/.downloads/pyinstaller/build-$MCP_OUTPUT_NAME" \
  --specpath "/workspace/.downloads/pyinstaller/spec-$MCP_OUTPUT_NAME" \
  --collect-all mcp_context_manager \
  src/mcp_context_manager/__main__.py
chown -R "$(stat -c "%u:%g" /workspace)" /workspace/dist /workspace/.downloads
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
