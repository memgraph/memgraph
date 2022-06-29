function(copy_util_e2e_python_files FILE_NAME)
    copy_e2e_python_files(write_procedures ${FILE_NAME})
endfunction()

copy_util_e2e_python_files(simple_case_nullcheck.py)
