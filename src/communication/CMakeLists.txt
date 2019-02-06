set(communication_src_files
    bolt/v1/value.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp)

add_library(mg-communication STATIC ${communication_src_files})
target_link_libraries(mg-communication Threads::Threads mg-utils mg-io fmt glog gflags)
target_link_libraries(mg-communication ${OPENSSL_LIBRARIES})
target_include_directories(mg-communication SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

set(communication_rpc_src_files
    rpc/client.cpp
    rpc/protocol.cpp
    rpc/server.cpp)

define_add_capnp(add_capnp communication_rpc_src_files communication_rpc_capnp_files)

add_capnp(rpc/messages.capnp)

add_custom_target(generate_communication_rpc_capnp DEPENDS ${communication_rpc_capnp_files})

add_library(mg-comm-rpc STATIC ${communication_rpc_src_files})
target_link_libraries(mg-comm-rpc Threads::Threads mg-communication mg-utils mg-io fmt glog gflags)
target_link_libraries(mg-comm-rpc capnp kj)

add_dependencies(mg-comm-rpc generate_communication_rpc_capnp)
