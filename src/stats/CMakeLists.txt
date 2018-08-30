set(stats_src_files
    metrics.cpp
    stats.cpp)

define_add_capnp(stats_src_files stats_capnp_files)
define_add_lcp(stats_src_files stats_lcp_files)

add_lcp(stats_rpc_messages.lcp CAPNP_SCHEMA @0xc19a87c81b9b4512)
add_capnp(stats_rpc_messages.capnp)

add_custom_target(generate_stats_lcp DEPENDS ${stats_lcp_files})
add_custom_target(generate_stats_capnp DEPENDS generate_stats_lcp ${stats_capnp_files})

add_library(mg-stats STATIC ${stats_src_files})
target_link_libraries(mg-stats Threads::Threads mg-utils mg-io mg-communication fmt glog gflags)
target_link_libraries(mg-stats capnp kj)
add_dependencies(mg-stats generate_stats_lcp)
add_dependencies(mg-stats generate_stats_capnp)
