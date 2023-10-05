add_library(mg-replication STATIC)
add_library(mg::replication ALIAS mg-replication)
target_sources(mg-replication
        PUBLIC
        include/replication/replication_state.hpp

        PRIVATE
        replication_state.cpp
)
target_include_directories(mg-replication PUBLIC include)

#target_link_libraries(mg-replication ...)
