add_library(mg-replication STATIC)
add_library(mg::replication ALIAS mg-replication)
target_sources(mg-replication
        PUBLIC
        include/replication/state.hpp
        include/replication/epoch.hpp
        include/replication/config.hpp
        include/replication/mode.hpp
        include/replication/role.hpp
        include/replication/status.hpp

        PRIVATE
        state.cpp
        epoch.cpp
        config.cpp
        status.cpp
)
target_include_directories(mg-replication PUBLIC include)

find_package(fmt REQUIRED)
target_link_libraries(mg-replication
    PUBLIC mg::utils mg::kvstore lib::json
    PRIVATE fmt::fmt
)
