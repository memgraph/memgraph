function(copy_fine_grained_access_e2e_python_files FILE_NAME)
    copy_e2e_python_files(fine_grained_access ${FILE_NAME})
endfunction()

copy_fine_grained_access_e2e_python_files(common.py)
copy_fine_grained_access_e2e_python_files(edge_type_filtering_tests.py)
