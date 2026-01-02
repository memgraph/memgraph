set(connectivity_module_src
    connectivity_module.cpp)

add_query_module(weakly_connected_components 1 ${connectivity_module_src})

# Link external libraries
target_link_libraries(weakly_connected_components PRIVATE mg_utility)
