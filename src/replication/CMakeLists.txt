add_library(mg-replication STATIC)
add_library(mg::replication ALIAS mg-replication)
target_sources(mg-replication
        PUBLIC
        include/replication/state.hpp
        include/replication/epoch.hpp
        include/replication/config.hpp
        include/replication/status.hpp
        include/replication/messages.hpp
        include/replication/replication_client.hpp
        include/replication/replication_server.hpp

        PRIVATE
        state.cpp
        epoch.cpp
        config.cpp
        status.cpp
        messages.cpp
        replication_client.cpp
        replication_server.cpp
)
target_include_directories(mg-replication PUBLIC include)

find_package(fmt REQUIRED)
target_link_libraries(mg-replication
    PUBLIC mg::utils mg::kvstore lib::json mg::rpc mg::slk mg::io mg::repl_coord_glue
    PRIVATE fmt::fmt
)
