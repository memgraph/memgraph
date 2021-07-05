function(copy_streams_e2e_python_files FILE_NAME)
add_custom_target(memgraph__e2e__streams__${FILE_NAME} ALL
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}.py
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}.py
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}.py)
endfunction()

copy_streams_e2e_python_files(start_stop_show)
add_subdirectory(procedures)