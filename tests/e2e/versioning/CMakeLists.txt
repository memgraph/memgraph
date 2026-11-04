function(copy_versioning_e2e_python_files FILE_NAME)
    copy_e2e_python_files(versioning ${FILE_NAME})
endfunction()

copy_e2e_python_files_from_parent_folder(versioning "../query_modules" common.py)
copy_versioning_e2e_python_files(versioning_bolt_db_test.py)
copy_versioning_e2e_python_files(versioning_mgp_test.py)

add_subdirectory(query_modules)

copy_e2e_files(versioning workloads.yaml)
