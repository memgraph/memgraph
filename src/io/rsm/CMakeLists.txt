set(io_rsm_sources
    raft.hpp
    rsm.hpp)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-io-rsm STATIC ${io_rsm_sources})
target_link_libraries(mg-io-rsm stdc++fs Threads::Threads fmt::fmt mg-utils mg-io)
