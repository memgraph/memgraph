function(copy_drop_graph_e2e_python_files FILE_NAME)
    copy_e2e_python_files(drop_graph ${FILE_NAME})
endfunction()

copy_drop_graph_e2e_python_files(common.py)
copy_drop_graph_e2e_python_files(drop_graph.py)
