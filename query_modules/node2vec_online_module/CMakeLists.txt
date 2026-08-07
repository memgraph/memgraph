find_package(Threads REQUIRED)

set(node2vec_online_src
    node2vec_online_module.cpp)

add_query_module(node2vec_online 1 "${node2vec_online_src}")

target_link_libraries(node2vec_online PRIVATE Threads::Threads)
