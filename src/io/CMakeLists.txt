set(io_src_files
    network/addrinfo.cpp
    network/endpoint.cpp
    network/socket.cpp
    network/utils.cpp)

add_library(mg-io STATIC ${io_src_files})
target_link_libraries(mg-io stdc++fs Threads::Threads fmt glog mg-utils)
# TODO: Remove this dependency when we switch to capnp
target_link_libraries(mg-io ${Boost_SERIALIZATION_LIBRARY_RELEASE})
