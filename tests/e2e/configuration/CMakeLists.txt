function(copy_configuration_check_e2e_python_files FILE_NAME)
    copy_e2e_python_files(configuration ${FILE_NAME})
endfunction()

copy_configuration_check_e2e_python_files(default_config.py)
copy_configuration_check_e2e_python_files(configuration_check.py)
copy_configuration_check_e2e_python_files(storage_info.py)

copy_e2e_files(configuration workloads.yaml)
