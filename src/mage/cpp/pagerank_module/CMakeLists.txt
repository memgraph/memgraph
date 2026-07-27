set(pagerank_module_src
    pagerank_module.cpp
    algorithm/pagerank.cpp)

mage_add_query_module(pagerank 1 "${pagerank_module_src}")

# Module tests
if (MG_MAGE_BUILD_TESTS)
    include(GoogleTest)
    set(pagerank_test_src
        pagerank_test.cpp
        algorithm/pagerank.cpp)

    add_executable(pagerank_test "${pagerank_test_src}")
    target_link_libraries(pagerank_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET pagerank_test TEST_PREFIX mage__)
endif()
