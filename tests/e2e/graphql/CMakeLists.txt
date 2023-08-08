function(copy_graphql_e2e_python_files FILE_NAME)
    copy_e2e_python_files(graphql ${FILE_NAME})
endfunction()

copy_graphql_e2e_python_files(graphql_crud.py)
copy_graphql_e2e_python_files(graphql_server.py)
copy_graphql_e2e_python_files(callable_alias_mapping.json)

add_subdirectory(graphql_library_config)
add_subdirectory(temporary_procedures)
