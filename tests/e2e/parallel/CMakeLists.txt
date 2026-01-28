message(STATUS "Processing parallel e2e tests...")

function(copy_parallel_e2e_python_files FILE_NAME)
    copy_e2e_python_files(parallel ${FILE_NAME})
endfunction()

# Copy Python test files
copy_parallel_e2e_python_files(conftest.py)
copy_parallel_e2e_python_files(common.py)
copy_parallel_e2e_python_files(test_exceptions.py)
copy_parallel_e2e_python_files(test_fgac.py)
copy_parallel_e2e_python_files(test_parallel_correctness.py)
copy_parallel_e2e_python_files(test_hops_limit.py)
copy_parallel_e2e_python_files(test_explain.py)
copy_parallel_e2e_python_files(test_termination.py)
copy_parallel_e2e_python_files(test_frame_change_collector.py)
copy_parallel_e2e_python_files(test_trigger_context_collector.py)
copy_parallel_e2e_python_files(test_profile.py)
copy_parallel_e2e_python_files(test_memory.py)
copy_parallel_e2e_python_files(test_procedures.py)

# Copy configuration and runner files
copy_e2e_files(parallel workloads.yaml)

# Copy helper scripts from parent folder
copy_e2e_python_files_from_parent_folder(parallel ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(parallel ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(parallel ".." mg_utils.py)

# Process procedures subdirectory
add_subdirectory(procedures)
