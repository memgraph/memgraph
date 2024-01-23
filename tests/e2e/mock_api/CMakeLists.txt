function(copy_mock_python_api_e2e_files FILE_NAME)
    copy_e2e_python_files(mock_python_api ${FILE_NAME})
endfunction()

add_subdirectory(procedures)

copy_mock_python_api_e2e_files(common.py)
copy_mock_python_api_e2e_files(test_compare_mock.py)

copy_e2e_files(mock_python_api workloads.yaml)
