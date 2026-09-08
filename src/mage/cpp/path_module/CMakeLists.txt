set(path_module_src
    path_module.cpp
    algorithm/path.cpp)

mage_add_query_module(path 1 "${path_module_src}")
