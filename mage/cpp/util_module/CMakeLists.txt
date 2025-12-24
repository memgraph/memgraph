set(util_module_src
    util_module.cpp
    algorithm/util.cpp
    algorithm/md5.cpp)

add_query_module(util_module 1 "${util_module_src}")
