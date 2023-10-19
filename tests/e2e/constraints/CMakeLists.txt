function(copy_analyze_graph_e2e_python_files FILE_NAME)
    copy_e2e_python_files(analyze_graph ${FILE_NAME})
endfunction()

copy_analyze_graph_e2e_python_files(common.py)
copy_analyze_graph_e2e_python_files(constraints_validation.py)
