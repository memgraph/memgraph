add_library(mg-dbms STATIC
        dbms_handler.cpp
        database.cpp
        coordinator_handler.cpp
        inmemory/replication_handlers.cpp
        replication_handlers.cpp
        rpc.cpp

)
target_link_libraries(mg-dbms mg-utils mg-storage-v2 mg-query mg-auth mg-replication mg-coordination)
