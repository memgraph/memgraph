set(algo_src
    algo_module.cpp
    algorithm/algo.cpp)

mage_add_query_module(algo 1 "${algo_src}")
