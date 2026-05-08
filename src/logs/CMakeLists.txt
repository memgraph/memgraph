find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)

add_library(mg-logs STATIC failed_query_log.cpp)
target_link_libraries(mg-logs
    PRIVATE fmt::fmt spdlog::spdlog)
target_include_directories(mg-logs PUBLIC ${CMAKE_SOURCE_DIR}/src)
