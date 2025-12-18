set(merge_src
    merge_module.cpp
    algorithm/merge.cpp)

add_query_module(merge 1 "${merge_src}")
