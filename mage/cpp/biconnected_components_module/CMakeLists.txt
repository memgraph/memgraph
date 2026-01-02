# Create a static library for the shared algorithm code
add_library(biconnected_components_algorithm STATIC
    algorithm/biconnected_components.cpp)
target_include_directories(biconnected_components_algorithm PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(biconnected_components_algorithm PRIVATE mg_utility)

set(biconnected_components_module_src
    biconnected_components_module.cpp)

add_query_module(biconnected_components 1 "${biconnected_components_module_src}")

# Link external libraries
target_link_libraries(biconnected_components PRIVATE mg_utility biconnected_components_algorithm)
target_include_directories(biconnected_components PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(biconnected_components_test_src
        biconnected_components_test.cpp)

    add_executable(biconnected_components_test  "${biconnected_components_test_src}")
    target_link_libraries(biconnected_components_test PRIVATE mg_utility mage_gtest biconnected_components_algorithm)
    gtest_add_tests(TARGET biconnected_components_test)
endif()
