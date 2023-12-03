function(copy_filter_info_e2e_python_files FILE_NAME)
    copy_e2e_python_files(filter_info ${FILE_NAME})
endfunction()

copy_filter_info_e2e_python_files(common.py)
copy_filter_info_e2e_python_files(filter_info.py)
