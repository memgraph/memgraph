add_library(mg-coordination STATIC)
add_library(mg::coordination ALIAS mg-coordination)
target_sources(mg-coordination
        PUBLIC
        include/coordination/coordinator_client.hpp
        include/coordination/coordinator_state.hpp
        include/coordination/coordinator_rpc.hpp
        include/coordination/coordinator_server.hpp
        include/coordination/coordinator_config.hpp
        include/coordination/coordinator_exceptions.hpp
        include/coordination/coordinator_instance.hpp
        include/coordination/coordinator_slk.hpp
        include/coordination/coordinator_data.hpp
        include/coordination/constants.hpp
        include/coordination/coordinator_cluster_config.hpp

        PRIVATE
        coordinator_client.cpp
        coordinator_state.cpp
        coordinator_rpc.cpp
        coordinator_server.cpp
        coordinator_data.cpp
        coordinator_instance.cpp
)
target_include_directories(mg-coordination PUBLIC include)

target_link_libraries(mg-coordination
    PUBLIC mg::utils mg::rpc mg::slk mg::io mg::repl_coord_glue lib::rangev3
)
