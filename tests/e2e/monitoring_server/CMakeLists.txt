find_package(gflags REQUIRED)
find_package(Boost REQUIRED)

add_library(monitoring_common common.cpp)
target_link_libraries(monitoring_common PUBLIC
    mgclient::mgclient mg-utils nlohmann_json::nlohmann_json gflags Boost::headers openssl::openssl
)

add_executable(memgraph__e2e__monitoring_server monitoring.cpp)
target_link_libraries(memgraph__e2e__monitoring_server  monitoring_common)

add_executable(memgraph__e2e__monitoring_server_ssl monitoring_ssl.cpp)
target_link_libraries(memgraph__e2e__monitoring_server_ssl monitoring_common)

copy_e2e_files(monitoring_server workloads.yaml)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
