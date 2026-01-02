set(node_similarity_module_src
    node_similarity_module.cpp
    algorithms/node_similarity.hpp)

add_query_module(node_similarity 1 "${node_similarity_module_src}")

# Link external libraries
target_link_libraries(node_similarity PRIVATE mg_utility)
target_include_directories(node_similarity PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
