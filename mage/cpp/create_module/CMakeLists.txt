set(create_module_src
    create_module.cpp
    algorithm/create.cpp)

add_query_module(create 1 "${create_module_src}")
