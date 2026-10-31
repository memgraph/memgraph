function(copy_versioning_e2e_python_files FILE_NAME)
    copy_e2e_python_files(versioning ${FILE_NAME})
endfunction()

copy_versioning_e2e_python_files(versioning_bolt_db_test.py)

copy_e2e_files(versioning workloads.yaml)
