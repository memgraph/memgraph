find_package(gflags REQUIRED)

add_executable(memgraph__e2e__replication__constraints constraints.cpp)
target_link_libraries(memgraph__e2e__replication__constraints gflags mgclient mg-utils mg-io Threads::Threads)

add_executable(memgraph__e2e__replication__indices indices.cpp)
target_link_libraries(memgraph__e2e__replication__indices gflags mgclient mg-utils mg-io Threads::Threads)

add_executable(memgraph__e2e__replication__read_write_benchmark read_write_benchmark.cpp)
target_link_libraries(memgraph__e2e__replication__read_write_benchmark gflags json mgclient mg-utils mg-io Threads::Threads)

copy_e2e_python_files(replication coordinator.py)
copy_e2e_python_files(replication_show common.py)
copy_e2e_python_files(replication_show conftest.py)
copy_e2e_python_files(replication_show show.py)
copy_e2e_python_files(replication_show show_while_creating_invalid_state.py)
copy_e2e_python_files(replication_show edge_delete.py)
copy_e2e_python_files_from_parent_folder(replication_show ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(replication_show ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(replication_show ".." mg_utils.py)
