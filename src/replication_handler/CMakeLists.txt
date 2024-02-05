add_library(mg-replication_handler STATIC)
add_library(mg::replication_handler ALIAS mg-replication_handler)
target_sources(mg-replication_handler
        PUBLIC
        include/replication_handler/replication_handler.hpp
        include/replication_handler/system_replication.hpp
        include/replication_handler/system_rpc.hpp

        PRIVATE
        replication_handler.cpp
        system_replication.cpp
        system_rpc.cpp
        )
target_include_directories(mg-replication_handler PUBLIC include)

target_link_libraries(mg-replication_handler
        PUBLIC mg-auth mg-dbms mg-replication)
