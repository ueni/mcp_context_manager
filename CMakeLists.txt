cmake_minimum_required(VERSION 3.20)

project(mcp-context-manager-build NONE)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

include(BuildArtifacts)

mcp_add_build_artifact_targets()
