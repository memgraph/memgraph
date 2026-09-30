add_library(mg-distributed)
add_library(mg::distributed ALIAS mg-distributed)
target_sources(mg-distributed
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/distributed/lamport_clock.hpp

    PRIVATE
    lamport_clock.cpp
)
