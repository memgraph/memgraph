add_library(mg-distributed)
add_library(mg::distributed ALIAS mg-distributed)
target_include_directories(mg-distributed PUBLIC include )
target_sources(mg-distributed PRIVATE lamport_clock.cpp)
