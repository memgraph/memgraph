find_package(gflags REQUIRED)

add_executable(memgraph__e2e__isolation_levels isolation_levels.cpp)
target_link_libraries(memgraph__e2e__isolation_levels gflags mgclient::mgclient mg-utils mg-io Threads::Threads)

copy_e2e_files(isolation_levels workloads.yaml)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
