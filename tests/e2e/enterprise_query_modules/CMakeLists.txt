function(copy_enterprise_query_modules_e2e_python_files FILE_NAME)
    copy_e2e_python_files(enterprise_query_modules ${FILE_NAME})
endfunction()

copy_enterprise_query_modules_e2e_python_files(common.py)
copy_enterprise_query_modules_e2e_python_files(test_enterprise.py)

add_subdirectory(query_modules)

copy_e2e_files(enterprise_query_modules workloads.yaml)
