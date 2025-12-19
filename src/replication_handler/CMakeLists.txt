add_library(mg-replication_handler STATIC)
add_library(mg::replication_handler ALIAS mg-replication_handler)
target_sources(mg-replication_handler
        PUBLIC
        include/replication_handler/data_instance_management_server_handlers.hpp
        include/replication_handler/replication_handler.hpp
        include/replication_handler/system_replication.hpp
        include/replication_handler/system_rpc.hpp

        PRIVATE
        data_instance_management_server_handlers.cpp
        system_replication.cpp
        replication_handler.cpp
        system_rpc.cpp
        )
target_include_directories(mg-replication_handler PUBLIC include)

target_link_libraries(mg-replication_handler
        PUBLIC mg-auth mg-coordination mg-dbms mg-replication)
