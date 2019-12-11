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

# integration test binaries
add_subdirectory(integration)
