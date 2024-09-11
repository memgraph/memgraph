function(copy_query_log_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_log ${FILE_NAME})
endfunction()

copy_query_log_e2e_python_files(common.py)
copy_query_log_e2e_python_files(query_log.py)

copy_e2e_files(query_log workloads.yaml)
