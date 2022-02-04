find_package(gflags REQUIRED)
find_package(Boost REQUIRED)

add_executable(memgraph__e2e__websocket websocket.cpp)
target_link_libraries(memgraph__e2e__websocket mgclient mg-utils gflags Boost::headers)
