add_library(mg-coordination STATIC)
add_library(mg::coordination ALIAS mg-coordination)
target_sources(mg-coordination
        PUBLIC
        FILE_SET CXX_MODULES
        FILES
        coordinator_cluster_state.cppm
        coordinator_communication_config.cppm
        coordinator_exceptions.cppm
        coordinator_instance.cppm
        coordinator_instance_aux.cppm
        coordinator_instance_client.cppm
        coordinator_instance_context.cppm
        coordinator_instance_connector.cppm
        coordinator_instance_management_server.cppm
        coordinator_instance_management_server_handlers.cppm
        coordinator_log_store.cppm
        coordinator_observer.cppm
        coordinator_ops_status.cppm
        coordinator_slk.cppm
        coordinator_state.cppm
        coordinator_state_machine.cppm
        coordinator_state_manager.cppm
        constants.cppm
        data_instance_context.cppm
        data_instance_management_server.cppm
        data_instance_management_server_handlers.cppm
        instance_state.cppm
        instance_status.cppm
        logger.cppm
        log_level.cppm
        logger_wrapper.cppm
        raft_state.cppm
        replication_instance_client.cppm
        replication_instance_connector.cppm
        replication_lag_info.cppm
        utils.cppm
        FILE_SET HEADERS
        FILES
        include/coordination/coordinator_rpc.hpp


        PRIVATE
        coordinator_cluster_state.cpp
        coordinator_communication_config.cpp
        coordinator_instance.cpp
        coordinator_instance_aux.cpp
        coordinator_instance_client.cpp
        coordinator_instance_connector.cpp
        coordinator_instance_context.cpp
        coordinator_instance_management_server.cpp
        coordinator_instance_management_server_handlers.cpp
        coordinator_log_store.cpp
        coordinator_observer.cpp
        coordinator_rpc.cpp
        coordinator_state.cpp
        coordinator_state_machine.cpp
        coordinator_state_manager.cpp
        data_instance_context.cpp
        data_instance_management_server.cpp
        data_instance_management_server_handlers.cpp
        raft_state.cpp
        replication_instance_client.cpp
        replication_instance_connector.cpp
        utils.cpp
)
target_include_directories(mg-coordination PUBLIC include)

find_package(range-v3 REQUIRED)

target_link_libraries(mg-coordination
    PUBLIC mg-events mg::utils mg::rpc mg::slk mg::io mg::repl_coord_glue range-v3::range-v3 nuraft mg-replication_handler
)
