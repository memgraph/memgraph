set(rpc_src_files
    client.cpp
    protocol.cpp
    server.cpp)

find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

add_library(mg-rpc STATIC ${rpc_src_files})
add_library(mg::rpc ALIAS mg-rpc)
target_link_libraries(mg-rpc Threads::Threads mg-communication mg-utils mg-io fmt::fmt gflags)
target_link_libraries(mg-rpc mg-slk)
