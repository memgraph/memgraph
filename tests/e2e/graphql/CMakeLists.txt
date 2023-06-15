function(copy_graphql_e2e_python_files FILE_NAME)
    copy_e2e_python_files(graphql ${FILE_NAME})
endfunction()

copy_graphql_e2e_python_files(graphql_create.py)
copy_graphql_e2e_python_files(graphql_server.py)

add_subdirectory(graphql_library_config)
add_subdirectory(temporary_procedures)
