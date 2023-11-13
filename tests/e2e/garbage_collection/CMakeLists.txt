function(garbage_collection_e2e_python_files FILE_NAME)
    copy_e2e_python_files(garbage_collection ${FILE_NAME})
endfunction()

garbage_collection_e2e_python_files(common.py)
garbage_collection_e2e_python_files(conftest.py)
garbage_collection_e2e_python_files(gc.py)

add_subdirectory(procedures)
