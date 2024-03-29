function(copy_init_file_flags_e2e_python_files FILE_NAME)
copy_e2e_files(init_file_flags ${FILE_NAME})
endfunction()

function(copy_init_file_flags_e2e_files FILE_NAME)
    copy_e2e_files(init_file_flags ${FILE_NAME})
endfunction()

copy_init_file_flags_e2e_python_files(init_file_setup.py)
copy_init_file_flags_e2e_python_files(init_data_file_setup.py)

copy_init_file_flags_e2e_files(init_file.cypherl)

copy_e2e_files(init_file_flags workloads.yaml)
