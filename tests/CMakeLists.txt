file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/unit)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/benchmark)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test_results/property_based)

# copy test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests)

# move test data data to the build directory
set(test_data "tests/data")
set(test_data_src "${CMAKE_SOURCE_DIR}/${test_data}")
set(test_data_dst "${CMAKE_BINARY_DIR}/${test_data}")
add_custom_target(test_data
                  COMMAND rm -rf ${test_data_dst}
                  COMMAND cp -r ${test_data_src} ${test_data_dst})
# -----------------------------------------------------------------------------

include_directories(${GTEST_INCLUDE_DIR})

# benchmark test binaries
add_subdirectory(benchmark)

# macro_benchmark test binaries
add_subdirectory(macro_benchmark)

# stress test binaries
add_subdirectory(stress)

# concurrent test binaries
add_subdirectory(concurrent)

# manual test binaries
add_subdirectory(manual)

# unit test binaries
add_subdirectory(unit)

# property based test binaries
add_subdirectory(property_based)
