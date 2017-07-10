cmake_minimum_required(VERSION 3.1)

project(${project_name}_tests)

enable_testing()

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/unit)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/benchmark)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/property_based)

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
add_subdirectory(${PROJECT_SOURCE_DIR}/benchmark)

# concurrent test binaries
add_subdirectory(${PROJECT_SOURCE_DIR}/concurrent)

# integration test binaries
add_subdirectory(${PROJECT_SOURCE_DIR}/integration)

# manual test binaries
add_subdirectory(${PROJECT_SOURCE_DIR}/manual)

# unit test binaries
add_subdirectory(${PROJECT_SOURCE_DIR}/unit)

# property based test binaries
include_directories(${CMAKE_SOURCE_DIR}/libs/rapidcheck/extras/gtest/include)
add_subdirectory(${PROJECT_SOURCE_DIR}/property_based)
