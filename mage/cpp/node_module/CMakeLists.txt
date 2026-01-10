set(node_module_src
    node_module.cpp
    algorithm/node.cpp)

add_query_module(node 1 "${node_module_src}")
