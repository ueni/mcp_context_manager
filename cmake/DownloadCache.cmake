include_guard(GLOBAL)

set(MCP_DOWNLOAD_CACHE_DIR
    "${CMAKE_SOURCE_DIR}/.downloads"
    CACHE PATH "Persistent local cache for package-manager and pip downloads"
)
if(DEFINED ENV{HOST_WORKSPACE_FOLDER} AND NOT "$ENV{HOST_WORKSPACE_FOLDER}" STREQUAL "")
    set(_mcp_default_docker_workspace "$ENV{HOST_WORKSPACE_FOLDER}")
else()
    set(_mcp_default_docker_workspace "${CMAKE_SOURCE_DIR}")
endif()
set(MCP_DOCKER_WORKSPACE_DIR
    "${_mcp_default_docker_workspace}"
    CACHE PATH "Host-visible workspace path mounted into Docker builder containers"
)
set(MCP_DOCKER_DOWNLOAD_CACHE_DIR
    "${MCP_DOCKER_WORKSPACE_DIR}/.downloads"
    CACHE PATH "Host-visible download cache path mounted into Docker builder containers"
)
set(MCP_USE_SUDO_DOCKER
    OFF
    CACHE BOOL "Run Docker through sudo env PATH=..."
)

function(mcp_prepare_download_cache)
    file(MAKE_DIRECTORY
        "${MCP_DOWNLOAD_CACHE_DIR}"
        "${MCP_DOWNLOAD_CACHE_DIR}/apt/cache/partial"
        "${MCP_DOWNLOAD_CACHE_DIR}/apt/lists/partial"
        "${MCP_DOWNLOAD_CACHE_DIR}/apk"
        "${MCP_DOWNLOAD_CACHE_DIR}/pip"
    )
endfunction()

function(mcp_docker_command out_var)
    if(MCP_USE_SUDO_DOCKER)
        set(_command sudo env "PATH=$ENV{PATH}" docker)
    else()
        set(_command docker)
    endif()
    set(${out_var} ${_command} PARENT_SCOPE)
endfunction()

function(mcp_docker_shell out_var)
    if(MCP_USE_SUDO_DOCKER)
        set(_command "sudo env \"PATH=$ENV{PATH}\" docker")
    else()
        set(_command "docker")
    endif()
    set(${out_var} "${_command}" PARENT_SCOPE)
endfunction()
