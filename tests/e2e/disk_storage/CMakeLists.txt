function(copy_disk_storage_e2e_python_files FILE_NAME)
    copy_e2e_python_files(disk_storage ${FILE_NAME})
endfunction()

copy_disk_storage_e2e_python_files(common.py)
copy_disk_storage_e2e_python_files(data_import.py)
