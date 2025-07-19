function(copy_query_modules_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_modules ${FILE_NAME})
endfunction()

add_query_module(module module.cpp)
copy_query_modules_e2e_python_files(temporal.py)
