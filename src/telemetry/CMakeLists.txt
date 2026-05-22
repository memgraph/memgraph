set(telemetry_src_files
  collectors.cpp
  telemetry.cpp)

add_library(mg-telemetry STATIC ${telemetry_src_files})
target_include_directories(mg-telemetry PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mg-telemetry PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/..)
target_link_libraries(mg-telemetry PRIVATE mg-requests mg-kvstore mg-utils mg-events mg-dbms)
