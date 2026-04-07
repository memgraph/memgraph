function(copy_slow_query_log_e2e_python_files FILE_NAME)
    copy_e2e_python_files(slow_query_log ${FILE_NAME})
endfunction()

copy_slow_query_log_e2e_python_files(common.py)
copy_slow_query_log_e2e_python_files(slow_query_log.py)

copy_e2e_files(slow_query_log workloads.yaml)
