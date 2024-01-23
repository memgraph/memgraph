function(copy_streams_e2e_python_files FILE_NAME)
    copy_e2e_python_files(streams ${FILE_NAME})
endfunction()

copy_streams_e2e_python_files(common.py)
copy_streams_e2e_python_files(conftest.py)
copy_streams_e2e_python_files(kafka_streams_tests.py)
copy_streams_e2e_python_files(streams_owner_tests.py)
copy_streams_e2e_python_files(pulsar_streams_tests.py)

add_subdirectory(transformations)

copy_e2e_python_files_from_parent_folder(streams ".." mg_utils.py)

copy_e2e_files(streams workloads.yaml)
