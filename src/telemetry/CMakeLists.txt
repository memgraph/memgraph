set(telemetry_src_files
    collectors.cpp
    telemetry.cpp
    system_info.cpp)

add_library(telemetry_lib STATIC ${telemetry_src_files})
target_link_libraries(telemetry_lib mg-requests mg-kvstore mg-utils)
option(MG_TELEMETRY_ID_OVERRIDE "Override for the telemetry ID" STRING)
if (MG_TELEMETRY_ID_OVERRIDE)
  message(WARNING "Using telemetry ID override: ${MG_TELEMETRY_ID_OVERRIDE}")
  target_compile_definitions(telemetry_lib PRIVATE MG_TELEMETRY_ID_OVERRIDE="${MG_TELEMETRY_ID_OVERRIDE}")
endif()
