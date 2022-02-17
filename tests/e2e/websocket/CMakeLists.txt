find_package(gflags REQUIRED)
find_package(Boost REQUIRED)

add_executable(memgraph__e2e__websocket websocket.cpp)
target_link_libraries(memgraph__e2e__websocket mgclient mg-utils json gflags Boost::headers)

add_executable(memgraph__e2e__websocket_ssl websocket_ssl.cpp)
target_link_libraries(memgraph__e2e__websocket_ssl mgclient mg-utils json gflags Boost::headers)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.crt DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.key DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
