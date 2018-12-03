set(utils_src_files
    demangle.cpp
    file.cpp
    signals.cpp
    thread.cpp
    thread/sync.cpp
    uuid.cpp
    watchdog.cpp)

define_add_capnp(add_capnp utils_src_files utils_capnp_files)

#add_capnp(serialization.capnp)

#add_custom_target(generate_utils_capnp DEPENDS ${utils_capnp_files})

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads fmt glog gflags uuid)
#target_link_libraries(mg-utils capnp kj)
#add_dependencies(mg-utils generate_utils_capnp)

target_compile_options(mg-utils PRIVATE -fPIC)