set(communication_src_files
    bolt/v1/value.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp
    rpc/client.cpp
    rpc/protocol.cpp
    rpc/server.cpp)

# TODO: Extract data_structures to library
set(communication_src_files ${communication_src_files}
    ${CMAKE_SOURCE_DIR}/src/data_structures/concurrent/skiplist_gc.cpp)

define_add_capnp(communication_src_files communication_capnp_files)

add_capnp(rpc/messages.capnp)

add_custom_target(generate_communication_capnp DEPENDS ${communication_capnp_files})

add_library(mg-communication STATIC ${communication_src_files})
target_link_libraries(mg-communication Threads::Threads mg-utils mg-io fmt glog gflags)
target_link_libraries(mg-communication ${OPENSSL_LIBRARIES})
target_include_directories(mg-communication SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
target_link_libraries(mg-communication capnp kj)
add_dependencies(mg-communication generate_communication_capnp)
