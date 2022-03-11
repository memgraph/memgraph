# Set up C++ functions for e2e tests
function(add_query_module target_name src)
  add_library(${target_name} SHARED ${src})
  SET_TARGET_PROPERTIES(${target_name} PROPERTIES PREFIX "")
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include)
endfunction()

# Set up Python functions for e2e tests
function(copy_magic_functions_e2e_python_files FILE_NAME)
    copy_e2e_python_files(functions ${FILE_NAME})
endfunction()

copy_magic_functions_e2e_python_files(common.py)
copy_magic_functions_e2e_python_files(conftest.py)
copy_magic_functions_e2e_python_files(function_example.py)

add_subdirectory(functions)
