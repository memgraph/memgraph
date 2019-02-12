define_add_capnp(add_capnp rpc_src_files rpc_capnp_files)

add_capnp(serialization.capnp)

add_custom_target(generate_rpc_capnp DEPENDS ${rpc_capnp_files})

add_library(mg-rpc STATIC ${rpc_src_files})
target_link_libraries(mg-rpc glog gflags capnp kj)
add_dependencies(mg-rpc generate_rpc_capnp)
