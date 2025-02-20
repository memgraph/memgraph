function(copy_composite_indices_e2e_python_files FILE_NAME)
    copy_e2e_python_files(composite_indices ${FILE_NAME})
endfunction()

copy_composite_indices_e2e_python_files(common.py)
copy_composite_indices_e2e_python_files(composite_indices.py)

copy_e2e_files(composite_indices workloads.yaml)
