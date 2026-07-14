set(node2vec_module_src
    node2vec_module.cpp)

add_query_module(node2vec 1 "${node2vec_module_src}")

# Link external libraries
target_link_libraries(node2vec PRIVATE mg_utility Threads::Threads)
target_include_directories(node2vec PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(node2vec_test_src
        node2vec_test.cpp)

    add_executable(node2vec_test "${node2vec_test_src}")
    target_link_libraries(node2vec_test PRIVATE mg_utility mage_gtest Threads::Threads)
    target_include_directories(node2vec_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    gtest_add_tests(TARGET node2vec_test)
endif()
