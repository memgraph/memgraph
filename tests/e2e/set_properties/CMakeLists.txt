function(copy_set_properties_e2e_python_files FILE_NAME)
    copy_e2e_python_files(set_properties ${FILE_NAME})
endfunction()

copy_set_properties_e2e_python_files(common.py)
copy_set_properties_e2e_python_files(set_properties.py)

add_subdirectory(procedures)
