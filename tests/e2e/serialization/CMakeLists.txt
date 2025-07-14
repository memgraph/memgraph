function(copy_serialization_e2e_python_files FILE_NAME)
    copy_e2e_python_files(serialization ${FILE_NAME})
endfunction()

copy_serialization_e2e_python_files(common.py)
copy_serialization_e2e_python_files(serialization.py)

copy_e2e_files(serialization workloads.yaml)
