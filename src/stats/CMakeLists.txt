set(stats_src_files
    metrics.cpp
    stats.cpp)

define_add_lcp(add_lcp stats_src_files stats_lcp_files)

add_lcp(stats_rpc_messages.lcp SLK_SERIALIZE)

add_custom_target(generate_stats_lcp DEPENDS ${stats_lcp_files})

add_library(mg-stats STATIC ${stats_src_files})
target_link_libraries(mg-stats Threads::Threads mg-utils mg-io mg-comm-rpc fmt glog gflags)
add_dependencies(mg-stats generate_stats_lcp)
