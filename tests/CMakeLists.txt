cmake_minimum_required(VERSION 3.1)

project(${project_name}_tests)

enable_testing()

include_directories(${catch_source_dir}/include)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results)

# copy test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests)

# move test data data to the build directory
if (UNIX)
    set(test_data "tests/data")
    set(test_data_src "${CMAKE_SOURCE_DIR}/${test_data}")
	set(test_data_dst "${CMAKE_BINARY_DIR}/${test_data}")
	add_custom_target (test_data
		COMMAND rm -rf ${test_data_dst}
        COMMAND cp -r ${test_data_src} ${test_data_dst}
	)
endif (UNIX)
# -----------------------------------------------------------------------------

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
