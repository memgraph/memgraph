set(pagerank_online_module_src
    pagerank_online_module.cpp
    algorithm_online/pagerank.cpp
)

add_query_module(pagerank_online 1 "${pagerank_online_module_src}")
