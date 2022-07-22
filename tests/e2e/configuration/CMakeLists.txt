function(copy_configuration_check_e2e_python_files FILE_NAME)
    copy_e2e_python_files(write_procedures ${FILE_NAME})
endfunction()

copy_write_procedures_e2e_python_files(default_config.py)
copy_write_procedures_e2e_python_files(configuration_check.py)
