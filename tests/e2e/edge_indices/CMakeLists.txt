function(copy_edge_indices_e2e_python_files FILE_NAME)
    copy_e2e_python_files(edge_indices ${FILE_NAME})
endfunction()

copy_edge_indices_e2e_python_files(common.py)
copy_edge_indices_e2e_python_files(edge_indices.py)

copy_e2e_files(edge_indices workloads.yaml)
