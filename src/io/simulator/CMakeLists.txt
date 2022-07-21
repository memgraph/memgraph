set(io_simulator_sources
    simulator.hpp
    simulator_handle.hpp
    simulator_stats.hpp
    simulator_config.hpp)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-io-simulator STATIC ${io_simulator_sources})
target_link_libraries(mg-io-simulator stdc++fs Threads::Threads fmt::fmt mg-utils)
