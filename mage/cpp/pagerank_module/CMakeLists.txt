set(pagerank_module_src
    pagerank_module.cpp
    algorithm/pagerank.cpp)

add_query_module(pagerank 1 "${pagerank_module_src}")

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(pagerank_test_src
        pagerank_test.cpp
        algorithm/pagerank.cpp)

    add_executable(pagerank_test "${pagerank_test_src}")
    target_link_libraries(pagerank_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET pagerank_test)
endif()
