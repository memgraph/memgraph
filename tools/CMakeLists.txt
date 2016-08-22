cmake_minimum_required(VERSION 3.1)

project(memgraph_tools)

add_executable(import_tool tool.cpp)
target_link_libraries(import_tool memgraph)
target_link_libraries(import_tool Threads::Threads)
target_link_libraries(import_tool ${fmt_static_lib})
