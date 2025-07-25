function(copy_using_unique_lock_e2e_python_files FILE_NAME)
    copy_e2e_python_files(using_unique_lock ${FILE_NAME})
endfunction()

copy_using_unique_lock_e2e_python_files(common.py)
copy_using_unique_lock_e2e_python_files(using_unique_lock.py)

copy_e2e_files(using_unique_lock workloads.yaml)
