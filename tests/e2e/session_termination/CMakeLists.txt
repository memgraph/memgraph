function(copy_session_termination_e2e_python_files FILE_NAME)
    copy_e2e_python_files(session_termination ${FILE_NAME})
endfunction()

copy_e2e_python_files_from_parent_folder(session_termination ".." auth_helpers.py)
copy_session_termination_e2e_python_files(common.py)
copy_session_termination_e2e_python_files(test_terminate_sessions.py)

copy_e2e_files(session_termination workloads.yaml)
