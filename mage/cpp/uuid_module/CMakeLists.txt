set(uuid_module_src
    uuid_module.cpp)

add_query_module(uuid_generator 1 ${uuid_module_src})

target_link_libraries(uuid_generator PRIVATE uuid)
