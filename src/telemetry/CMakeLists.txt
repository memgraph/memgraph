set(telemetry_src_files
    collectors.cpp
    telemetry.cpp
    system_info.cpp)

add_library(telemetry_lib STATIC ${telemetry_src_files})
target_link_libraries(telemetry_lib mg-requests mg-kvstore mg-utils)
if (BUILD_FOR_DOCKER)
  target_compile_definitions(telemetry_lib PRIVATE DOCKER_BUILD)
endif()
