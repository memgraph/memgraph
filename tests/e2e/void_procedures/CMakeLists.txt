function(copy_void_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(void_procedures ${FILE_NAME})
endfunction()

copy_void_procedures_e2e_python_files(common.py)
copy_void_procedures_e2e_python_files(conftest.py)
copy_void_procedures_e2e_python_files(void_procedures_test.py)

add_subdirectory(procedures)

copy_e2e_files(void_procedures workloads.yaml)
