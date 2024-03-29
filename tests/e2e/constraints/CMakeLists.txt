function(copy_constraint_validation_e2e_python_files FILE_NAME)
    copy_e2e_python_files(constraint_validation ${FILE_NAME})
endfunction()

copy_constraint_validation_e2e_python_files(common.py)
copy_constraint_validation_e2e_python_files(constraints_validation.py)

copy_e2e_files(constraint_validation workloads.yaml)
