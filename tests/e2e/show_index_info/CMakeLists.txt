function(copy_show_index_info_e2e_python_files FILE_NAME)
    copy_e2e_python_files(show_index_info ${FILE_NAME})
endfunction()

copy_show_index_info_e2e_python_files(common.py)
copy_show_index_info_e2e_python_files(test_show_index_info.py)

copy_e2e_files(show_index_info workloads.yaml)
