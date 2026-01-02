set(label_module_src
    label_module.cpp
    algorithm/label.cpp)

add_query_module(label 1 "${label_module_src}")
