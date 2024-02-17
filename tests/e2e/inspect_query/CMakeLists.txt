function(copy_inspect_query_e2e_python_files FILE_NAME)
    copy_e2e_python_files(inspect_query ${FILE_NAME})
endfunction()

copy_inspect_query_e2e_python_files(common.py)
copy_inspect_query_e2e_python_files(inspect_query.py)

copy_e2e_files(inspect_query workloads.yaml)
