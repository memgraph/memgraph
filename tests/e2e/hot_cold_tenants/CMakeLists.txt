copy_e2e_python_files(hot_cold_tenants common.py)
copy_e2e_python_files(hot_cold_tenants hot_cold_tenants.py)

copy_e2e_python_files_from_parent_folder(hot_cold_tenants ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(hot_cold_tenants ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(hot_cold_tenants ".." mg_utils.py)

copy_e2e_files(hot_cold_tenants workloads.yaml)
