function(copy_index_hints_e2e_python_files FILE_NAME)
    copy_e2e_python_files(index_hints ${FILE_NAME})
endfunction()

copy_index_hints_e2e_python_files(common.py)
copy_index_hints_e2e_python_files(index_hints.py)

copy_e2e_files(index_hints workloads.yaml)
