function(copy_show_privileges_e2e_python_files FILE_NAME)
    copy_e2e_python_files(show_privileges ${FILE_NAME})
endfunction()

copy_show_privileges_e2e_python_files(common.py)
copy_show_privileges_e2e_python_files(show_privileges.py)

copy_e2e_files(show_privileges workloads.yaml)
