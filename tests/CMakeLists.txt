cmake_minimum_required(VERSION 3.1)

project(${project_name}_tests)

enable_testing()

include_directories(${catch_source_dir}/include)

# copy unit test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests)
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests/unit)

# benchmark test binaries
if (ALL_TESTS OR BENCHMARK_TESTS)
    add_subdirectory(${PROJECT_SOURCE_DIR}/benchmark)
endif()

# concurrent test binaries
if (ALL_TESTS OR CONCURRENT_TESTS)
    add_subdirectory(${PROJECT_SOURCE_DIR}/concurrent)
endif()

# integration test binaries
if (ALL_TESTS OR INTEGRATION_TESTS)
    add_subdirectory(${PROJECT_SOURCE_DIR}/integration)
endif()

# manual test binaries
if (ALL_TESTS OR MANUAL_TESTS)
    add_subdirectory(${PROJECT_SOURCE_DIR}/manual)
endif()

# unit test binaries
if (ALL_TESTS OR UNIT_TESTS)
    add_subdirectory(${PROJECT_SOURCE_DIR}/unit)
endif()
