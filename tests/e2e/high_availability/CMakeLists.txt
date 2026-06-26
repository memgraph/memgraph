find_package(gflags REQUIRED)

copy_e2e_python_files(high_availability coords_request_forwarding.py)
copy_e2e_python_files(high_availability replication_lag.py)
copy_e2e_python_files(high_availability strict_sync.py)
copy_e2e_python_files(high_availability coordinator.py)
copy_e2e_python_files(high_availability single_coordinator.py)
copy_e2e_python_files(high_availability coord_cluster_registration.py)
copy_e2e_python_files(high_availability distributed_coords.py)
copy_e2e_python_files(high_availability defunct_replica_self_heal.py)
copy_e2e_python_files(high_availability disable_writing_on_main_after_restart.py)
copy_e2e_python_files(high_availability durability.py)
copy_e2e_python_files(high_availability defunct_cure_flows.py)
copy_e2e_python_files(high_availability manual_setting_replicas.py)
copy_e2e_python_files(high_availability ttl.py)
copy_e2e_python_files(high_availability common.py)
copy_e2e_python_files(high_availability auth.py)
copy_e2e_python_files(high_availability large_cluster.py)
copy_e2e_python_files(high_availability instance_metrics.py)
copy_e2e_python_files(high_availability tls_cluster.py)
copy_e2e_python_files(high_availability workloads.yaml)


copy_e2e_python_files_from_parent_folder(high_availability ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(high_availability ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(high_availability ".." mg_utils.py)

# Copy the entire tls_certs/ directory (CA + per-instance leaf certs and keys)
# to the build dir. tls_cluster.py resolves cert paths via
# `interactive_mg_runner.SCRIPT_DIR / "tls_certs"` at runtime.
add_custom_target(memgraph__e2e__high_availability__tls_certs_dir ALL
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/tls_certs
    ${CMAKE_CURRENT_BINARY_DIR}/tls_certs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tls_certs)
add_dependencies(memgraph__e2e memgraph__e2e__high_availability__tls_certs_dir)

if (MG_ENTERPRISE)
add_executable(memgraph__e2e__high_availability_rpc_comm rpc_comm.cpp)
target_link_libraries(memgraph__e2e__high_availability_rpc_comm mg-coordination mg-repl_coord_glue)
endif()
