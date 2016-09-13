cmake_minimum_required(VERSION 3.1)

project(memgraph_poc)


include_directories(${CMAKE_SOURCE_DIR}/poc)


add_executable(poc_astar astar.cpp)
target_link_libraries(poc_astar memgraph)
target_link_libraries(poc_astar Threads::Threads)
target_link_libraries(poc_astar ${fmt_static_lib})
target_link_libraries(poc_astar ${yaml_static_lib})

add_executable(profile profile.cpp)
target_link_libraries(profile memgraph)
target_link_libraries(profile Threads::Threads)
target_link_libraries(profile ${fmt_static_lib})
target_link_libraries(profile ${yaml_static_lib})

add_executable(csv_import csv_import.cpp)
target_link_libraries(csv_import memgraph)
target_link_libraries(csv_import Threads::Threads)
target_link_libraries(csv_import ${fmt_static_lib})
target_link_libraries(csv_import ${yaml_static_lib})

add_executable(add_double add_double.cpp)
target_link_libraries(add_double memgraph)
target_link_libraries(add_double Threads::Threads)
target_link_libraries(add_double ${fmt_static_lib})
target_link_libraries(add_double ${yaml_static_lib})

add_executable(astar_query astar_query.cpp)
target_link_libraries(astar_query memgraph)
target_link_libraries(astar_query Threads::Threads)
target_link_libraries(astar_query ${fmt_static_lib})
target_link_libraries(astar_query ${yaml_static_lib})

add_executable(size_aligment size_aligment.cpp)
target_link_libraries(size_aligment memgraph)
target_link_libraries(size_aligment Threads::Threads)
target_link_libraries(size_aligment ${fmt_static_lib})
target_link_libraries(size_aligment ${yaml_static_lib})

add_executable(isolation isolation.cpp isolation/header.cpp)
target_link_libraries(isolation ${fmt_static_lib})
