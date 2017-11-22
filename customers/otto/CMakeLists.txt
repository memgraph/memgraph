set(exec_name customers_otto_parallel_connected_components)
add_executable(${exec_name} parallel_connected_components.cpp)
target_link_libraries(${exec_name} memgraph_lib)
