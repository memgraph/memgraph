add_library(mg-dbms STATIC)

target_sources(mg-dbms
    PRIVATE
    dbms_handler.cpp
    database.cpp
    coordinator_handler.cpp
    inmemory/replication_handlers.cpp
    replication_handlers.cpp
    rpc.cpp
    database_protector.cpp

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS .
    FILES
    database_protector.hpp
)

target_link_libraries(mg-dbms mg-utils mg::storage mg-query mg-auth mg-replication mg-coordination)
