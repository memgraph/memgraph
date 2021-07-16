function(copy_streams_e2e_python_files FILE_NAME)
add_custom_target(memgraph__e2e__streams__${FILE_NAME} ALL
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
endfunction()

copy_streams_e2e_python_files(common.py)
copy_streams_e2e_python_files(conftest.py)
copy_streams_e2e_python_files(streams_tests.py)
copy_streams_e2e_python_files(streams_owner_tests.py)
copy_streams_e2e_python_files(streams_test_runner.sh)
add_subdirectory(transformations)
