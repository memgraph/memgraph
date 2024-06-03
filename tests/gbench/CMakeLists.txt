find_package(benchmark REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(bench disk.cpp)

target_link_libraries(bench benchmark::benchmark ${CMAKE_THREAD_LIBS_INIT} mg-utils mg-storage-v2)
target_include_directories(bench PRIVATE "${PROJECT_SOURCE_DIR}/include")
install(TARGETS bench RUNTIME DESTINATION bin/tests)
