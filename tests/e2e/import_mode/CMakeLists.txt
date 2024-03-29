function(copy_import_mode_e2e_python_files FILE_NAME)
    copy_e2e_python_files(import_mode ${FILE_NAME})
endfunction()

copy_import_mode_e2e_python_files(common.py)
copy_import_mode_e2e_python_files(test_command.py)

copy_e2e_files(import_mode workloads.yaml)
