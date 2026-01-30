set(katz_src
    katz_centrality_module.cpp
    algorithm/katz.cpp)

add_query_module(katz_centrality 1 "${katz_src}")

# Link external libraries
target_link_libraries(katz_centrality PRIVATE mg_utility)
target_include_directories(katz_centrality PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
