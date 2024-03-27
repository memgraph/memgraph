add_library(mg-coordination STATIC)
add_library(mg::coordination ALIAS mg-coordination)
target_sources(mg-coordination
        PUBLIC
        include/coordination/coordinator_client.hpp
        include/coordination/coordinator_state.hpp
        include/coordination/coordinator_rpc.hpp
        include/coordination/coordinator_server.hpp
        include/coordination/coordinator_communication_config.hpp
        include/coordination/coordinator_exceptions.hpp
        include/coordination/coordinator_slk.hpp
        include/coordination/coordinator_instance.hpp
        include/coordination/coordinator_handlers.hpp
        include/coordination/instance_status.hpp
        include/coordination/replication_instance.hpp
        include/coordination/raft_state.hpp
        include/coordination/rpc_errors.hpp

        include/nuraft/raft_log_action.hpp
        include/nuraft/coordinator_cluster_state.hpp
        include/nuraft/coordinator_log_store.hpp
        include/nuraft/coordinator_state_machine.hpp
        include/nuraft/coordinator_state_manager.hpp

        PRIVATE
        coordinator_communication_config.cpp
        coordinator_client.cpp
        coordinator_state.cpp
        coordinator_rpc.cpp
        coordinator_server.cpp
        coordinator_handlers.cpp
        coordinator_instance.cpp
        replication_instance.cpp
        raft_state.cpp

        coordinator_log_store.cpp
        coordinator_state_machine.cpp
        coordinator_state_manager.cpp
        coordinator_cluster_state.cpp
)
target_include_directories(mg-coordination PUBLIC include)

target_link_libraries(mg-coordination
    PUBLIC mg::utils mg::rpc mg::slk mg::io mg::repl_coord_glue lib::rangev3 nuraft mg-replication_handler
)
