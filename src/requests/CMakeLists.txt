set(requests_src_files
    requests.cpp)

find_package(CURL REQUIRED)

add_library(mg-requests STATIC ${requests_src_files})
target_link_libraries(mg-requests spdlog fmt gflags json ${CURL_LIBRARIES})
target_include_directories(mg-requests PRIVATE ${CURL_INCLUDE_DIRS})
