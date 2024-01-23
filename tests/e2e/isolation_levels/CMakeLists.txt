find_package(gflags REQUIRED)

add_executable(memgraph__e2e__isolation_levels isolation_levels.cpp)
target_link_libraries(memgraph__e2e__isolation_levels gflags mgclient mg-utils mg-io Threads::Threads)

copy_e2e_files(isolation_levels workloads.yaml)
