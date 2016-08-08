cmake_minimum_required(VERSION 3.1)

project(memgraph_poc)

add_executable(poc_astar astar.cpp)
target_link_libraries(poc_astar memgraph)
