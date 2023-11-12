function(copy_queries_e2e_python_files FILE_NAME)
    copy_e2e_python_files(queries ${FILE_NAME})
endfunction()

copy_queries_e2e_python_files(common.py)
copy_queries_e2e_python_files(queries.py)
