add_library(mg-dbms STATIC dbms_handler.cpp database.cpp replication_handler.cpp replication_client.cpp inmemory/replication_handlers.cpp)
target_link_libraries(mg-dbms mg-utils mg-storage-v2 mg-query)
