find_package(gflags REQUIRED)
find_package(Boost REQUIRED)

add_executable(memgraph__e2e__monitoring_server monitoring.cpp)
target_link_libraries(memgraph__e2e__monitoring_server mgclient mg-utils json gflags Boost::headers)

add_executable(memgraph__e2e__monitoring_server_ssl monitoring_ssl.cpp)
target_link_libraries(memgraph__e2e__monitoring_server_ssl mgclient mg-utils json gflags Boost::headers)

copy_e2e_files(monitoring_server workloads.yaml)
