set(coordinator_src_files
    coordinator.hpp
    shard_map.hpp
    hybrid_logical_clock.hpp)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-coordinator STATIC ${coordinator_src_files})
target_link_libraries(mg-coordinator stdc++fs Threads::Threads fmt::fmt mg-utils)
