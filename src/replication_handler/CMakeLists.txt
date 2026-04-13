add_library(mg-replication_handler STATIC)
add_library(mg::replication_handler ALIAS mg-replication_handler)
target_sources(mg-replication_handler
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS include
        FILES
        include/replication_handler/replication_handler.hpp
        include/replication_handler/system_replication.hpp
        include/replication_handler/system_rpc.hpp

        PRIVATE
        system_replication.cpp
        replication_handler.cpp
        system_rpc.cpp
)

target_link_libraries(mg-replication_handler
        PUBLIC mg-auth mg-dbms mg-replication mg-coordination)
