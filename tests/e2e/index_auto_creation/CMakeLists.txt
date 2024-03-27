function(copy_auto_index_queries_e2e_python_files FILE_NAME)
    copy_e2e_python_files(index_auto_creation ${FILE_NAME})
endfunction()

copy_auto_index_queries_e2e_python_files(common.py)
copy_auto_index_queries_e2e_python_files(index_auto_creation.py)
