function(copy_auth_e2e_python_files FILE_NAME)
    copy_e2e_python_files(auth ${FILE_NAME})
endfunction()

copy_auth_e2e_python_files(common.py)
copy_auth_e2e_python_files(auth_queries.py)

copy_e2e_files(auth workloads.yaml)
