function(copy_vector_index_return_e2e_python_files FILE_NAME)
    copy_e2e_python_files(vector_index_return ${FILE_NAME})
endfunction()

copy_vector_index_return_e2e_python_files(omit_vector_index_properties.py)

copy_e2e_files(vector_index_return workloads.yaml)
