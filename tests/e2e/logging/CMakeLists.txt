function(copy_logging_e2e_python_files FILE_NAME)
    copy_e2e_python_files(logging ${FILE_NAME})
endfunction()

copy_logging_e2e_python_files(session_trace.py)
copy_logging_e2e_python_files(slow_failed_query.py)

copy_e2e_files(logging workloads.yaml)
