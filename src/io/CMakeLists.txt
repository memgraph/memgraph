set(io_src_files
    network/addrinfo.cpp
    network/endpoint.cpp
    network/socket.cpp
    network/utils.cpp)

add_library(mg-io STATIC ${io_src_files})
target_link_libraries(mg-io stdc++fs Threads::Threads fmt glog mg-utils)

set(io_serialization_src_files)

define_add_capnp(add_capnp io_serialization_src_files io_capnp_files)
add_capnp(network/endpoint.capnp)
add_custom_target(generate_io_capnp DEPENDS ${io_capnp_files})

add_library(mg-io-serialization STATIC ${io_serialization_src_files})
target_link_libraries(mg-io-serialization mg-io capnp kj)
add_dependencies(mg-io-serialization generate_io_capnp)
