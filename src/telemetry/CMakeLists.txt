set(telemetry_src_files
  collectors.cpp
  telemetry.cpp)

add_library(mg-telemetry STATIC ${telemetry_src_files})
target_include_directories(mg-telemetry PRIVATE ${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/..)
target_link_libraries(mg-telemetry mg-requests mg-kvstore mg-utils mg-events mg-dbms)
option(MG_TELEMETRY_ID_OVERRIDE "Override for the telemetry ID" STRING)

if(MG_TELEMETRY_ID_OVERRIDE)
  message(WARNING "Using telemetry ID override: ${MG_TELEMETRY_ID_OVERRIDE}")
  target_compile_definitions(mg-telemetry PRIVATE MG_TELEMETRY_ID_OVERRIDE="${MG_TELEMETRY_ID_OVERRIDE}")
endif()
