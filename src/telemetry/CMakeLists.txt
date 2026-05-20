set(telemetry_src_files
  collectors.cpp
  telemetry.cpp)

add_library(mg-telemetry STATIC ${telemetry_src_files})
target_include_directories(mg-telemetry PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mg-telemetry PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/..)
target_link_libraries(mg-telemetry PRIVATE mg-requests mg-kvstore mg-utils mg-events mg-dbms)

# The telemetry ID is resolved at runtime — see utils/system_info.cpp's
# GetMachineId(): MEMGRAPH_TELEMETRY_ID env var wins over /etc/machine-id.
# Container Dockerfiles set the env var to a stable identifier so a single
# memgraph build serves both the standalone deb (machine-id) and the
# container image (env override).
