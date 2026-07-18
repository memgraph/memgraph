# Add all module files related to cypher module
set(cypher_module_src
    cypher_module.cpp)

add_query_module(cypher 1 "${cypher_module_src}")

target_link_libraries(cypher PRIVATE fmt::fmt)
