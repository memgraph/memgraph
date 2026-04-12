add_library(mg-parameters STATIC)
add_library(mg::parameters ALIAS mg-parameters)

target_sources(mg-parameters
    PRIVATE
    parameters.cpp
    rpc.cpp
    replication_handlers.cpp
)

target_include_directories(mg-parameters PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(mg-parameters PUBLIC mg-kvstore mg-utils mg-slk mg-replication_handler)
