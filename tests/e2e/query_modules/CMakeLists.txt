function(copy_query_modules_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_modules ${FILE_NAME})
endfunction()

copy_query_modules_e2e_python_files(common.py)
copy_query_modules_e2e_python_files(conftest.py)
copy_query_modules_e2e_python_files(convert_test.py)
copy_query_modules_e2e_python_files(mgps_test.py)
copy_query_modules_e2e_python_files(schema_test.py)

copy_e2e_files(query_modules workloads.yaml)
