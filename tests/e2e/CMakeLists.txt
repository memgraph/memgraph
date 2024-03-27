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

function(copy_e2e_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
endfunction()

add_subdirectory(fine_grained_access)
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
add_subdirectory(metadata_queries)
add_subdirectory(module_file_manager)
add_subdirectory(monitoring_server)
add_subdirectory(lba_procedures)
add_subdirectory(python_query_modules_reloading)
add_subdirectory(analyze_graph)
add_subdirectory(transaction_queue)
add_subdirectory(mock_api)
add_subdirectory(disk_storage)
add_subdirectory(load_csv)
add_subdirectory(init_file_flags)
add_subdirectory(analytical_mode)
add_subdirectory(batched_procedures)
add_subdirectory(import_mode)
add_subdirectory(concurrent_query_modules)
add_subdirectory(show_index_info)
add_subdirectory(set_properties)
add_subdirectory(transaction_rollback)
add_subdirectory(index_hints)
add_subdirectory(query_modules)
add_subdirectory(constraints)
add_subdirectory(inspect_query)
add_subdirectory(filter_info)
add_subdirectory(queries)
add_subdirectory(query_modules_storage_modes)
add_subdirectory(garbage_collection)
add_subdirectory(query_planning)
add_subdirectory(awesome_functions)
add_subdirectory(high_availability)
add_subdirectory(concurrency)
add_subdirectory(execution_query_modules)

add_subdirectory(replication_experimental)

copy_e2e_python_files(pytest_runner pytest_runner.sh "")
copy_e2e_python_files(x x.sh "")
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.crt DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.key DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
