function(copy_query_modules_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_modules ${FILE_NAME})
endfunction()

copy_query_modules_e2e_python_files(common.py)
copy_query_modules_e2e_python_files(conftest.py)
copy_query_modules_e2e_python_files(query_modules.py)

add_subdirectory(procedures)
