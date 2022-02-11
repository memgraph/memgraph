set(requests_src_files
    requests.cpp)

find_package(CURL REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)


add_library(mg-requests STATIC ${requests_src_files})
target_link_libraries(mg-requests mg-utils spdlog::spdlog fmt::fmt gflags json ${CURL_LIBRARIES})
target_include_directories(mg-requests PRIVATE ${CURL_INCLUDE_DIRS})
