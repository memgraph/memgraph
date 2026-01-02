# Add all module files related to graph util module
set(graph_util_src
    graph_util_module.cpp)

add_query_module(graph_util 1 "${graph_util_src}")
