set(io_v3_sources
    address.hpp
    errors.hpp
    future.hpp
    transport.hpp
    simulator.hpp)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-io-v3 STATIC ${io_v3_sources})
target_link_libraries(mg-io-v3 stdc++fs Threads::Threads fmt::fmt mg-utils)
