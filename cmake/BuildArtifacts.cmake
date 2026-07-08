include_guard(GLOBAL)

include(DownloadCache)

set(MCP_DIST_DIR
    "${CMAKE_SOURCE_DIR}/dist"
    CACHE PATH "Directory for generated release artifacts"
)
set(MCP_DOCKER_IMAGE
    "mcp-context-manager:local"
    CACHE STRING "Docker image tag used by local build targets"
)
set(MCP_DOCKER_EXTRA_IMAGE_TAGS
    ""
    CACHE STRING "Optional semicolon-separated extra Docker image tags"
)
set(MCP_SERVER_BINARY
    "dist/mcp-context-manager-linux-x86_64-musl"
    CACHE STRING "Repository-relative musl executable path copied into the Docker image"
)
set(MCP_IMAGE_ARCHIVE
    "${MCP_DIST_DIR}/mcp-context-manager-local-image.tar.gz"
    CACHE FILEPATH "Path for the local Docker image archive"
)

function(mcp_collect_python_sources out_var)
    file(GLOB_RECURSE _sources CONFIGURE_DEPENDS
        "${CMAKE_SOURCE_DIR}/src/*.py"
        "${CMAKE_SOURCE_DIR}/pyproject.toml"
    )
    set(${out_var} ${_sources} PARENT_SCOPE)
endfunction()

function(mcp_add_standalone_binary runtime image output_name)
    mcp_collect_python_sources(_python_sources)
    set(_output "${MCP_DIST_DIR}/${output_name}")
    add_custom_command(
        OUTPUT "${_output}"
        COMMAND "${CMAKE_COMMAND}"
            "-DMCP_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
            "-DMCP_DIST_DIR=${MCP_DIST_DIR}"
            "-DMCP_DOWNLOAD_CACHE_DIR=${MCP_DOWNLOAD_CACHE_DIR}"
            "-DMCP_DOCKER_WORKSPACE_DIR=${MCP_DOCKER_WORKSPACE_DIR}"
            "-DMCP_DOCKER_DOWNLOAD_CACHE_DIR=${MCP_DOCKER_DOWNLOAD_CACHE_DIR}"
            "-DMCP_USE_SUDO_DOCKER=${MCP_USE_SUDO_DOCKER}"
            "-DMCP_RUNTIME=${runtime}"
            "-DMCP_CONTAINER_IMAGE=${image}"
            "-DMCP_OUTPUT_NAME=${output_name}"
            -P "${CMAKE_SOURCE_DIR}/cmake/BuildStandalone.cmake"
        DEPENDS ${_python_sources}
            "${CMAKE_SOURCE_DIR}/cmake/BuildStandalone.cmake"
            "${CMAKE_SOURCE_DIR}/cmake/DownloadCache.cmake"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        COMMENT "Building ${output_name} with cached downloads in ${MCP_DOWNLOAD_CACHE_DIR}"
        VERBATIM
    )
    add_custom_target("${runtime}-standalone" DEPENDS "${_output}")
    set("MCP_${runtime}_OUTPUT" "${_output}" PARENT_SCOPE)
endfunction()

function(mcp_add_build_artifact_targets)
    mcp_prepare_download_cache()
    mcp_docker_command(_docker_command)
    mcp_docker_shell(_docker_shell)

    mcp_add_standalone_binary(
        glibc
        ubuntu:22.04
        mcp-context-manager-linux-x86_64-glibc
    )
    mcp_add_standalone_binary(
        musl
        python:3.12-alpine
        mcp-context-manager-linux-x86_64-musl
    )

    add_custom_target(
        standalone-executable
        DEPENDS glibc-standalone musl-standalone
    )

    set(_docker_tag_args -t "${MCP_DOCKER_IMAGE}")
    foreach(_extra_tag IN LISTS MCP_DOCKER_EXTRA_IMAGE_TAGS)
        if(NOT _extra_tag STREQUAL "")
            list(APPEND _docker_tag_args -t "${_extra_tag}")
        endif()
    endforeach()

    add_custom_target(
        docker-image
        COMMAND ${_docker_command} build
            --build-arg "SERVER_BINARY=${MCP_SERVER_BINARY}"
            ${_docker_tag_args}
            "${CMAKE_SOURCE_DIR}"
        DEPENDS musl-standalone
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        COMMENT "Building Docker image ${MCP_DOCKER_IMAGE}"
        VERBATIM
    )

    add_custom_target(
        docker-image-archive
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${MCP_DIST_DIR}"
        COMMAND bash -ec
            "${_docker_shell} save '${MCP_DOCKER_IMAGE}' | gzip -9 > '${MCP_IMAGE_ARCHIVE}'"
        DEPENDS docker-image
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        COMMENT "Writing Docker image archive ${MCP_IMAGE_ARCHIVE}"
        VERBATIM
    )

    get_filename_component(_image_archive_name "${MCP_IMAGE_ARCHIVE}" NAME)
    add_custom_target(
        local-release-artifacts
        COMMAND bash -ec
            "cd '${MCP_DIST_DIR}' && sha256sum mcp-context-manager-linux-x86_64-glibc mcp-context-manager-linux-x86_64-musl '${_image_archive_name}' > SHA256SUMS"
        DEPENDS standalone-executable docker-image-archive
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        COMMENT "Creating local release checksums"
        VERBATIM
    )
endfunction()
