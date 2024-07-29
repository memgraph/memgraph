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

# e2e test binaries
add_subdirectory(e2e)

# mgbench benchmark test binaries
add_subdirectory(mgbench)

# Easy way to allow run-time check if built was with enterprise on
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/enterprise_check.sh.in ${CMAKE_CURRENT_BINARY_DIR}/enterprise_check.sh @ONLY)
add_custom_target(memgraph__tests__enterprise_check ALL
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/enterprise_check.sh
)
