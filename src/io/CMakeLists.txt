set(io_src_files
    network/addrinfo.cpp
    network/endpoint.cpp
    network/socket.cpp
    network/utils.cpp)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-io STATIC ${io_src_files})
add_library(mg::io ALIAS mg-io)
target_link_libraries(mg-io stdc++fs Threads::Threads fmt::fmt mg-utils)
