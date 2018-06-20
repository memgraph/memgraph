set(telemetry_src_files
    collectors.cpp
    requests.cpp
    telemetry.cpp
    system_info.cpp)

find_package(CURL REQUIRED)

add_library(telemetry_lib STATIC ${telemetry_src_files})
target_link_libraries(telemetry_lib glog json ${CURL_LIBRARIES} kvstore_lib)
target_include_directories(telemetry_lib PRIVATE ${CURL_INCLUDE_DIRS})
