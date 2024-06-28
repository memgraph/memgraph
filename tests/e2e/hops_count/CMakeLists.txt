function(copy_hops_count_e2e_python_files FILE_NAME)
    copy_e2e_python_files(hops_count ${FILE_NAME})
endfunction()

copy_hops_count_e2e_python_files(hops_count.py)

copy_e2e_files(hops_count workloads.yaml)
