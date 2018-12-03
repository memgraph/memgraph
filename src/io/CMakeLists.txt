set(io_src_files
    network/addrinfo.cpp
    network/endpoint.cpp
    network/socket.cpp
    network/utils.cpp)

define_add_capnp(add_capnp io_src_files io_capnp_files)

add_capnp(network/endpoint.capnp)

add_custom_target(generate_io_capnp DEPENDS ${io_capnp_files})

add_library(mg-io STATIC ${io_src_files})
target_link_libraries(mg-io stdc++fs Threads::Threads fmt glog mg-utils)
target_link_libraries(mg-io capnp kj)
add_dependencies(mg-io generate_io_capnp)

target_compile_options(mg-io PRIVATE -fPIC)
