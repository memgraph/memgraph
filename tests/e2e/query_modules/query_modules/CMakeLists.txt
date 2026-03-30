function(copy_query_modules_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_modules ${FILE_NAME})
endfunction()

add_query_module(module module.cpp)
add_query_module(type_annotations_cpp type_annotations_cpp.cpp)
copy_query_modules_e2e_python_files(temporal.py)
copy_query_modules_e2e_python_files(type_annotations.py)
copy_query_modules_e2e_python_files(vector_index_config.py)
