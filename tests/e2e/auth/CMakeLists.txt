function(copy_auth_e2e_python_files FILE_NAME)
    copy_e2e_python_files(auth ${FILE_NAME})
endfunction()

copy_e2e_python_files_from_parent_folder(auth ".." auth_helpers.py)
copy_auth_e2e_python_files(common.py)
copy_auth_e2e_python_files(auth_queries.py)
copy_auth_e2e_python_files(test_show_privileges_isolation.py)
copy_auth_e2e_python_files(test_auth_transactions.py)
copy_auth_e2e_python_files(test_dropped_session_principal.py)

copy_e2e_files(auth workloads.yaml)
