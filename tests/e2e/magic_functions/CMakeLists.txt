# Set up Python functions for e2e tests
function(copy_magic_functions_e2e_python_files FILE_NAME)
    copy_e2e_python_files(functions ${FILE_NAME})
endfunction()

copy_magic_functions_e2e_python_files(common.py)
copy_magic_functions_e2e_python_files(conftest.py)
copy_magic_functions_e2e_python_files(function_example.py)

add_subdirectory(functions)

copy_e2e_files(functions workloads.yaml)
