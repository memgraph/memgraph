find_package(gflags REQUIRED)

add_executable(memgraph__e2e__replication__constraints constraints.cpp)
target_link_libraries(memgraph__e2e__replication__constraints gflags mgclient::mgclient mg-utils mg-io Threads::Threads)
add_dependencies(memgraph__e2e memgraph__e2e__replication__constraints)

add_executable(memgraph__e2e__replication__indices indices.cpp)
target_link_libraries(memgraph__e2e__replication__indices gflags mgclient::mgclient mg-utils mg-io Threads::Threads)
add_dependencies(memgraph__e2e memgraph__e2e__replication__indices)

add_executable(memgraph__e2e__replication__read_write_benchmark read_write_benchmark.cpp)
target_link_libraries(memgraph__e2e__replication__read_write_benchmark gflags nlohmann_json::nlohmann_json mgclient::mgclient mg-utils mg-io Threads::Threads)
add_dependencies(memgraph__e2e memgraph__e2e__replication__read_write_benchmark)

copy_e2e_python_files(replication common.py)
copy_e2e_python_files(replication conftest.py)
copy_e2e_python_files(replication show.py)
copy_e2e_python_files(replication bolt_exception_handling.py)
copy_e2e_python_files(replication show_while_creating_invalid_state.py)
copy_e2e_python_files(replication edge_delete.py)
copy_e2e_python_files(replication replicate_enum.py)
copy_e2e_python_files(replication replicate_periodic_commit.py)
copy_e2e_python_files(replication replication_with_property_compression_used.py)
copy_e2e_python_files(replication replicate_spatial_feature.py)
copy_e2e_python_files(replication replicate_vector_index.py)
copy_e2e_python_files(replication replicate_vector_edge_index.py)
copy_e2e_python_files(replication replicate_descriptions.py)
copy_e2e_python_files(replication replicate_text_index.py)
copy_e2e_python_files(replication switching_roles.py)
copy_e2e_python_files(replication ttl.py)
copy_e2e_python_files_from_parent_folder(replication ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(replication ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(replication ".." mg_utils.py)

copy_e2e_files(replication workloads.yaml)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
