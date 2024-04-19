function(copy_qm_storage_modes_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_modules_storage_modes ${FILE_NAME})
endfunction()

copy_qm_storage_modes_e2e_python_files(common.py)
copy_qm_storage_modes_e2e_python_files(test_query_modules_storage_modes.py)

add_subdirectory(query_modules)

copy_e2e_files(query_modules_storage_modes workloads.yaml)
