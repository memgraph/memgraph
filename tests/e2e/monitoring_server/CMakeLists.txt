find_package(gflags REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)

add_library(monitoring_common common.cpp)
target_link_libraries(monitoring_common PUBLIC
    mgclient mg-utils nlohmann_json::nlohmann_json gflags Boost::headers openssl::openssl
)

add_executable(memgraph__e2e__monitoring_server monitoring.cpp)
target_link_libraries(memgraph__e2e__monitoring_server  monitoring_common)

add_executable(memgraph__e2e__monitoring_server_ssl monitoring_ssl.cpp)
target_link_libraries(memgraph__e2e__monitoring_server_ssl monitoring_common)

copy_e2e_files(monitoring_server workloads.yaml)
