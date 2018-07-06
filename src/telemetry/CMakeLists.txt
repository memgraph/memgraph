set(telemetry_src_files
    collectors.cpp
    telemetry.cpp
    system_info.cpp)

add_library(telemetry_lib STATIC ${telemetry_src_files})
target_link_libraries(telemetry_lib glog mg-requests kvstore_lib)
