set(map_module_src
    map_module.cpp
    algorithm/map.cpp)

add_query_module(map 1 "${map_module_src}")

target_link_libraries(map PRIVATE fmt::fmt)
