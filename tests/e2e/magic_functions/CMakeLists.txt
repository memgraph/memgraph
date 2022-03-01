# Set up C++ functions for e2e tests
add_library(mg_procedure INTERFACE)
target_include_directories(mg_procedure INTERFACE ${CMAKE_SOURCE_DIR}/include)

function(add_query_module target_name src)
  add_library(${target_name} SHARED ${src})
  SET_TARGET_PROPERTIES(${target_name} PROPERTIES PREFIX "")
  target_link_libraries(${target_name} PRIVATE mg_procedure)
endfunction()

# Set up Python functions for e2e tests
function(copy_magic_functions_e2e_python_files FILE_NAME)
    copy_e2e_python_files(functions ${FILE_NAME})
endfunction()

copy_magic_functions_e2e_python_files(common.py)
copy_magic_functions_e2e_python_files(conftest.py)
copy_magic_functions_e2e_python_files(function_example.py)

add_subdirectory(functions)
