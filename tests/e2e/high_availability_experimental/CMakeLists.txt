find_package(gflags REQUIRED)

copy_e2e_python_files(ha_experimental coordinator.py)
copy_e2e_python_files(ha_experimental client_initiated_failover.py)
copy_e2e_python_files(ha_experimental uninitialized_cluster.py)
copy_e2e_python_files(ha_experimental common.py)
copy_e2e_python_files(ha_experimental conftest.py)
copy_e2e_python_files(ha_experimental workloads.yaml)

copy_e2e_python_files_from_parent_folder(ha_experimental ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(ha_experimental ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(ha_experimental ".." mg_utils.py)
