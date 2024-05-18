function(copy_hops_limit_e2e_python_files FILE_NAME)
    copy_e2e_python_files(hops_limit ${FILE_NAME})
endfunction()

copy_hops_limit_e2e_python_files(count_hops.py)

copy_e2e_files(hops_limit workloads.yaml)
