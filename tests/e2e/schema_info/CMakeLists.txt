function(copy_schema_info_e2e_python_files FILE_NAME)
    copy_e2e_python_files(schema_info ${FILE_NAME})
endfunction()

copy_schema_info_e2e_python_files(common.py)
copy_schema_info_e2e_python_files(schema_info.py)

copy_e2e_python_files_from_parent_folder(schema_info ".." mg_utils.py)

copy_e2e_files(schema_info workloads.yaml)
