set(meta_module_src
    meta_module.cpp
    algorithm/meta.cpp)

add_query_module(meta 1 "${meta_module_src}")
