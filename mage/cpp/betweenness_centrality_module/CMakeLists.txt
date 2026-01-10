# Offline betweenness centrality module

set(betweenness_centrality_src
    betweenness_centrality_module.cpp
    algorithm/betweenness_centrality.cpp)

add_query_module(betweenness_centrality 1 "${betweenness_centrality_src}")

# Link external libraries
target_link_libraries(betweenness_centrality PRIVATE mg_utility)
target_include_directories(betweenness_centrality PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(betweenness_centrality_src
        betweenness_centrality_test.cpp
        algorithm/betweenness_centrality.cpp)

    add_executable(betweenness_centrality_test "${betweenness_centrality_src}")
    target_link_libraries(betweenness_centrality_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET betweenness_centrality_test)
endif()

######################################################################################

# Online betweenness centrality module

set(betweenness_centrality_online_src
    betweenness_centrality_online_module.cpp
    algorithm/betweenness_centrality.cpp
    algorithm_online/betweenness_centrality_online.cpp)

add_query_module(betweenness_centrality_online 1 "${betweenness_centrality_online_src}")

# Link external libraries
target_link_libraries(betweenness_centrality_online PRIVATE mg_utility biconnected_components_algorithm)
target_include_directories(betweenness_centrality_online PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(betweenness_centrality_online_test_src
        betweenness_centrality_online_test.cpp
        algorithm/betweenness_centrality.cpp
        algorithm_online/betweenness_centrality_online.cpp)

    add_executable(betweenness_centrality_online_test "${betweenness_centrality_online_test_src}")
    target_link_libraries(betweenness_centrality_online_test PRIVATE mg_utility mage_gtest biconnected_components_algorithm)
    gtest_add_tests(TARGET betweenness_centrality_online_test)
endif()
