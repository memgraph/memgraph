function(copy_failed_query_log_e2e_python_files FILE_NAME)
    copy_e2e_python_files(failed_query_log ${FILE_NAME})
endfunction()

copy_failed_query_log_e2e_python_files(common.py)
copy_failed_query_log_e2e_python_files(failed_query_log.py)

copy_e2e_files(failed_query_log workloads.yaml)
