set(nodes_module_src
    nodes_module.cpp
    algorithm/nodes.cpp)

add_query_module(nodes 1 "${nodes_module_src}")
