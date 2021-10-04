set(telemetry_src_files
    collectors.cpp
    telemetry.cpp
    system_info.cpp)

add_library(telemetry_lib STATIC ${telemetry_src_files})
target_link_libraries(telemetry_lib mg-requests mg-kvstore mg-utils)
option(TELEMETRY_ID STRING)
if (TELEMETRY_ID)
  target_compile_definitions(telemetry_lib PRIVATE TELEMETRY_ID=${TELEMETRY_ID})
endif()
