# Set up C++ functions for e2e tests
function(add_query_module target_name src)
        add_library(${target_name} SHARED ${src})
        SET_TARGET_PROPERTIES(${target_name} PROPERTIES PREFIX "")
        target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include)
endfunction()

add_custom_target(memgraph__e2e)
add_dependencies(memgraph__e2e memgraph)

function(copy_e2e_python_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
        add_dependencies(memgraph__e2e memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME})
endfunction()

function(copy_e2e_python_files_from_parent_folder TARGET_PREFIX EXTRA_PATH FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${EXTRA_PATH}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${EXTRA_PATH}/${FILE_NAME})
        add_dependencies(memgraph__e2e memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME})
endfunction()

function(copy_e2e_cpp_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
        add_dependencies(memgraph__e2e memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME})
endfunction()

function(copy_e2e_files TARGET_PREFIX FILE_NAME)
        add_custom_target(memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME} ALL
                COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}
                DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_NAME})
        add_dependencies(memgraph__e2e memgraph__e2e__${TARGET_PREFIX}__${FILE_NAME})
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
add_subdirectory(load_parquet)
add_subdirectory(load_jsonl)
add_subdirectory(init_file_flags)
add_subdirectory(analytical_mode)
add_subdirectory(batched_procedures)
add_subdirectory(import_mode)
add_subdirectory(concurrent_query_modules)
add_subdirectory(show_index_info)
add_subdirectory(set_properties)
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
add_subdirectory(drop_queries)
add_subdirectory(concurrent_write)
add_subdirectory(concurrency)
add_subdirectory(execution_query_modules)
add_subdirectory(index_auto_creation)
add_subdirectory(user_changing_its_own_password)
add_subdirectory(sso)
add_subdirectory(auth)
add_subdirectory(hops_count)
add_subdirectory(observe_current_amount_of_deltas)
add_subdirectory(show_metrics)
add_subdirectory(schema_info)
add_subdirectory(time_to_live)
add_subdirectory(show_active_users_info)
add_subdirectory(periodic_commit)
add_subdirectory(query_log)
add_subdirectory(skiplist_indices)
add_subdirectory(system_replication)
add_subdirectory(durability)
add_subdirectory(show_license_info)
add_subdirectory(enterprise_query_modules)
add_subdirectory(dynamic_variable_creation)
add_subdirectory(graphql)
add_subdirectory(show_privileges)
add_subdirectory(kshortest_limit)
add_subdirectory(user_profiles)

copy_e2e_python_files(pytest_runner pytest_runner.sh "")
copy_e2e_python_files(x x.sh "")
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.crt DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/memgraph-selfsigned.key DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
