function(copy_drop_queries_e2e_python_files FILE_NAME)
    copy_e2e_python_files(drop_queries ${FILE_NAME})
endfunction()

copy_drop_queries_e2e_python_files(common.py)
copy_drop_queries_e2e_python_files(drop_graph.py)
copy_drop_queries_e2e_python_files(drop_all_indexes.py)

copy_e2e_files(drop_queries workloads.yaml)
