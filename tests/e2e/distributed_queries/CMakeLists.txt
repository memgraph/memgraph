function(distributed_queries_e2e_python_files FILE_NAME)
    copy_e2e_python_files(distributed_queries ${FILE_NAME})
endfunction()

distributed_queries_e2e_python_files(distributed_queries.py)
distributed_queries_e2e_python_files(common.py)
