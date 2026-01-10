set(math_module_src
    math_module.cpp
    algorithm/math.cpp)

add_query_module(math 1 "${math_module_src}")
