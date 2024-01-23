
find_package(gflags REQUIRED)

add_executable(memgraph__e2e__temporal_roundtrip roundtrip.cpp)
target_link_libraries(memgraph__e2e__temporal_roundtrip PUBLIC mgclient mg-utils gflags)

copy_e2e_files(temporal_roundtrip workloads.yaml)
