find_package(gflags REQUIRED)

copy_e2e_python_files(durability conftest.py)
copy_e2e_python_files(durability common.py)
copy_e2e_python_files(durability durability_with_property_compression_used.py)
copy_e2e_python_files(durability durability_with_text_index.py)
copy_e2e_python_files(durability periodic_snapshot.py)
copy_e2e_python_files(durability snapshot_recovery.py)
copy_e2e_python_files(durability multitenancy.py)

copy_e2e_python_files_from_parent_folder(durability ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(durability ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(durability ".." mg_utils.py)

copy_e2e_files(durability workloads.yaml)
