function(copy_streams_e2e_python_files FILE_NAME)
    copy_e2e_python_files(streams ${FILE_NAME})
endfunction()

copy_streams_e2e_python_files(common.py)
copy_streams_e2e_python_files(conftest.py)
copy_streams_e2e_python_files(streams_tests.py)
copy_streams_e2e_python_files(streams_owner_tests.py)

add_subdirectory(transformations)
