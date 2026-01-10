set(degree_centrality_module_src
    degree_centrality_module.cpp
    algorithm/degree_centrality.cpp)

add_query_module(degree_centrality 1 "${degree_centrality_module_src}")

# Link external libraries
target_link_libraries(degree_centrality PRIVATE mg_utility)
target_include_directories(degree_centrality PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(degree_centrality_test_src
        degree_centrality_test.cpp
        algorithm/degree_centrality.cpp)

    add_executable(degree_centrality_test "${degree_centrality_test_src}")
    target_link_libraries(degree_centrality_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET degree_centrality_test)
endif()
