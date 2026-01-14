message(STATUS "Processing parallel e2e tests...")

function(copy_parallel_e2e_python_files FILE_NAME)
    copy_e2e_python_files(parallel ${FILE_NAME})
endfunction()

# Copy Python test files
copy_parallel_e2e_python_files(conftest.py)
copy_parallel_e2e_python_files(common.py)
copy_parallel_e2e_python_files(test_exceptions.py)
copy_parallel_e2e_python_files(test_fgac.py)

# Copy configuration and runner files
copy_e2e_files(parallel workloads.yaml)

# Copy helper scripts from parent folder
copy_e2e_python_files_from_parent_folder(parallel ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(parallel ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(parallel ".." mg_utils.py)
