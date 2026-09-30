set(katz_online_src
    katz_centrality_online_module.cpp
    algorithm/katz.cpp)

add_query_module(katz_centrality_online 1 "${katz_online_src}")

target_include_directories(katz_centrality_online PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
