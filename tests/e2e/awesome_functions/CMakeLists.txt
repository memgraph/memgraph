function(copy_awesome_functions_e2e_python_files FILE_NAME)
    copy_e2e_python_files(awesome_functions ${FILE_NAME})
endfunction()

copy_awesome_functions_e2e_python_files(common.py)
copy_awesome_functions_e2e_python_files(property_size.py)
copy_awesome_functions_e2e_python_files(value_type.py)
copy_awesome_functions_e2e_python_files(id.py)
copy_awesome_functions_e2e_python_files(to_set.py)

copy_e2e_files(awesome_functions workloads.yaml)
