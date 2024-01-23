find_package(gflags REQUIRED)

copy_e2e_python_files(replication_experiment common.py)
copy_e2e_python_files(replication_experiment conftest.py)
copy_e2e_python_files(replication_experiment multitenancy.py)
copy_e2e_python_files_from_parent_folder(replication_experiment ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(replication_experiment ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(replication_experiment ".." mg_utils.py)

copy_e2e_files(replication_experiment workloads.yaml)
