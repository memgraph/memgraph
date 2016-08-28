cmake_minimum_required(VERSION 3.1)

project(memgraph_poc)

add_executable(poc_astar astar.cpp)
target_link_libraries(poc_astar memgraph)
target_link_libraries(poc_astar Threads::Threads)
target_link_libraries(poc_astar ${fmt_static_lib})

add_executable(profile profile.cpp)
target_link_libraries(profile memgraph)
target_link_libraries(profile Threads::Threads)
target_link_libraries(profile ${fmt_static_lib})

include_directories(${CMAKE_SOURCE_DIR}/poc)

add_executable(isolation isolation.cpp isolation/header.cpp)
target_link_libraries(isolation ${fmt_static_lib})

add_executable(size_aligment size_aligment.cpp)
target_link_libraries(size_aligment memgraph)
target_link_libraries(size_aligment Threads::Threads)
target_link_libraries(size_aligment ${fmt_static_lib})
