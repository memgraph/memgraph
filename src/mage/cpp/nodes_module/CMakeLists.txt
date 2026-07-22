set(nodes_module_src
    nodes_module.cpp
    algorithm/nodes.cpp)

mage_add_query_module(nodes 1 "${nodes_module_src}")
