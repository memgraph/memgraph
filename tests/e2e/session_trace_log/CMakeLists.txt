function(copy_session_trace_log_e2e_python_files FILE_NAME)
    copy_e2e_python_files(session_trace_log ${FILE_NAME})
endfunction()

copy_session_trace_log_e2e_python_files(session_trace_log.py)

copy_e2e_files(session_trace_log workloads.yaml)
