set(bipartite_matching_src
    bipartite_matching_module.cpp
    algorithm/bipartite_matching.cpp)

mage_add_query_module(bipartite_matching 1 "${bipartite_matching_src}")

# Link external libraries
target_link_libraries(bipartite_matching PRIVATE mg_utility)
target_include_directories(bipartite_matching PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (MG_MAGE_BUILD_TESTS)
    include(GoogleTest)
    set(bipartite_matching_test_src
        bipartite_matching_test.cpp
        algorithm/bipartite_matching.cpp)

    add_executable(bipartite_matching_test  "${bipartite_matching_test_src}")
    target_link_libraries(bipartite_matching_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET bipartite_matching_test TEST_PREFIX mage__)
endif()
