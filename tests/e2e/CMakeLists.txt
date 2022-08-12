# Set up C++ functions for e2e tests
function(add_query_module target_name src)
        add_library(${target_name} SHARED ${src})
        SET_TARGET_PROPERTIES(${target_name} PROPERTIES PREFIX "")
        target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include)
endfunction()

function(copy_e2e_python_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
endfunction()

function(copy_e2e_python_files_from_parent_folder TARGET_PREFIX EXTRA_PATH FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${EXTRA_PATH}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${EXTRA_PATH}/${FILE_NAME})
endfunction()

function(copy_e2e_cpp_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
endfunction()

add_subdirectory(server)
add_subdirectory(replication)
add_subdirectory(memory)
add_subdirectory(triggers)
add_subdirectory(isolation_levels)
add_subdirectory(streams)
add_subdirectory(temporal_types)
add_subdirectory(write_procedures)
add_subdirectory(configuration)
add_subdirectory(magic_functions)
add_subdirectory(module_file_manager)
add_subdirectory(monitoring_server)

copy_e2e_python_files(pytest_runner pytest_runner.sh "")
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.crt DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.key DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
