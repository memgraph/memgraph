cmake_minimum_required(VERSION 3.1)

project(memgraph_benchmark)

add_executable(skiplist_benchmark skiplist.cpp)
target_link_libraries(skiplist_benchmark Threads::Threads)
target_link_libraries(skiplist_benchmark memgraph)
target_link_libraries(skiplist_benchmark ${fmt_static_lib})
