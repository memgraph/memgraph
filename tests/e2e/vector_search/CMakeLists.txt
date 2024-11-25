function(copy_vector_search_e2e_python_files FILE_NAME)
    copy_e2e_python_files(vector_search ${FILE_NAME})
endfunction()

copy_vector_search_e2e_python_files(common.py)
copy_vector_search_e2e_python_files(test_vector_search.py)

copy_e2e_files(vecctor_search workloads.yaml)
