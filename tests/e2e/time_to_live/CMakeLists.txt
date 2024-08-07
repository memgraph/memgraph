function(copy_time_to_live_e2e_python_files FILE_NAME)
    copy_e2e_python_files(time_to_live ${FILE_NAME})
endfunction()

copy_time_to_live_e2e_python_files(common.py)
copy_time_to_live_e2e_python_files(multitenancy.py)

copy_e2e_python_files_from_parent_folder(time_to_live ".." mg_utils.py)

copy_e2e_files(time_to_live workloads.yaml)
