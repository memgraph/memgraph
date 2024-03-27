function(copy_concurrent_write_e2e_python_files FILE_NAME)
    copy_e2e_python_files(concurrent_write ${FILE_NAME})
endfunction()

copy_concurrent_write_e2e_python_files(write.py)

copy_e2e_files(concurrent_write workloads.yaml)
