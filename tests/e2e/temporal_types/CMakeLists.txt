
find_package(gflags REQUIRED)

add_executable(memgraph__e2e__temporal_roundtrip roundtrip.cpp)
target_link_libraries(memgraph__e2e__temporal_roundtrip PUBLIC mgclient::mgclient mg-utils gflags)

copy_e2e_files(temporal_roundtrip workloads.yaml)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
