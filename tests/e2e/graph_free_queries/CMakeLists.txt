function(copy_graph_free_queries_e2e_python_files FILE_NAME)
    copy_e2e_python_files(graph_free_queries ${FILE_NAME})
endfunction()

copy_graph_free_queries_e2e_python_files(common.py)
copy_graph_free_queries_e2e_python_files(test_dropped_database.py)

copy_e2e_files(graph_free_queries workloads.yaml)
