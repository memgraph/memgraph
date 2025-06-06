add_library(mg-coordination STATIC)
add_library(mg::coordination ALIAS mg-coordination)
target_sources(mg-coordination
        PUBLIC
        include/coordination/constants.hpp
        include/coordination/coordination_observer.hpp
        include/coordination/coordinator_cluster_state.hpp
        include/coordination/coordinator_communication_config.hpp
        include/coordination/coordinator_exceptions.hpp
        include/coordination/coordinator_instance.hpp
        include/coordination/coordinator_instance_aux.hpp
        include/coordination/coordinator_instance_client.hpp
        include/coordination/coordinator_instance_connector.hpp
        include/coordination/coordinator_instance_context.hpp
        include/coordination/coordinator_instance_management_server.hpp
        include/coordination/coordinator_instance_management_server_handlers.hpp
        include/coordination/coordinator_log_store.hpp
        include/coordination/coordinator_rpc.hpp
        include/coordination/coordinator_slk.hpp
        include/coordination/coordinator_state.hpp
        include/coordination/coordinator_state_machine.hpp
        include/coordination/coordinator_state_manager.hpp
        include/coordination/data_instance_context.hpp
        include/coordination/data_instance_management_server.hpp
        include/coordination/data_instance_management_server_handlers.hpp
        include/coordination/instance_state.hpp
        include/coordination/instance_status.hpp
        include/coordination/logger.hpp
        include/coordination/logger_wrapper.hpp
        include/coordination/raft_state.hpp
        include/coordination/replication_instance_client.hpp
        include/coordination/replication_instance_connector.hpp
        include/coordination/utils.hpp


        PRIVATE
        coordination_observer.cpp
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
        coordinator_rpc.cpp
        coordinator_state.cpp
        coordinator_state_machine.cpp
        coordinator_state_manager.cpp
        data_instance_context.cpp
        data_instance_management_server.cpp
        data_instance_management_server_handlers.cpp
        logger.cpp
        logger_wrapper.cpp
        raft_state.cpp
        replication_instance_client.cpp
        replication_instance_connector.cpp
        utils.cpp
)
target_include_directories(mg-coordination PUBLIC include)

target_link_libraries(mg-coordination
    PUBLIC mg::utils mg::rpc mg::slk mg::io mg::repl_coord_glue lib::rangev3 nuraft mg-replication_handler
)
