find_package(gflags REQUIRED)

copy_e2e_python_files(durability common.py)
copy_e2e_files(durability workloads.yaml)
