function(copy_e2e_python_files TARGET_PREFIX FILE_NAME)
add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
endfunction()

add_subdirectory(replication)
add_subdirectory(memory)
add_subdirectory(triggers)
add_subdirectory(isolation_levels)
add_subdirectory(streams)
add_subdirectory(temporal_types)
add_subdirectory(write_procedures)
add_subdirectory(websocket)

copy_e2e_python_files(pytest_runner pytest_runner.sh "")
