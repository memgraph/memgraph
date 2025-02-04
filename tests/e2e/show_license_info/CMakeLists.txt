function(copy_show_license_info_e2e_python_files FILE_NAME)
    copy_e2e_python_files(show_license_info ${FILE_NAME})
endfunction()

copy_show_license_info_e2e_python_files(common.py)
copy_show_license_info_e2e_python_files(test_show_license_info.py)

copy_e2e_files(show_license_info workloads.yaml)
