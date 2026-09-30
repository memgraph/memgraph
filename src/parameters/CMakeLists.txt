add_library(mg-parameters STATIC)
add_library(mg::parameters ALIAS mg-parameters)

target_sources(mg-parameters
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ..
    FILES
    parameters.hpp
    rpc.hpp

    PRIVATE
    parameters.cpp
    rpc.cpp
)

target_link_libraries(mg-parameters PUBLIC mg-kvstore mg-utils mg-slk mg::system)
