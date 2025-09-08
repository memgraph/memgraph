function(copy_kshortest_limit_e2e_python_files FILE_NAME)
    copy_e2e_python_files(kshortest_limit ${FILE_NAME})
endfunction()

copy_kshortest_limit_e2e_python_files(kshortest_limit.py)

copy_e2e_files(kshortest_limit workloads.yaml)
