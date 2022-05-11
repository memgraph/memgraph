find_package(gflags REQUIRED)
find_package(Boost REQUIRED)

add_executable(memgraph__e2e__monitoring_server monitoring_websocket.cpp)
target_link_libraries(memgraph__e2e__monitoring_server mgclient mg-utils json gflags Boost::headers)

add_executable(memgraph__e2e__monitoring_server_ssl monitoring_websocket_ssl.cpp)
target_link_libraries(memgraph__e2e__monitoring_server_ssl mgclient mg-utils json gflags Boost::headers)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.crt DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.key DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
