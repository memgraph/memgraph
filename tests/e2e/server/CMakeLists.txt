find_package(gflags REQUIRED)
find_package(Boost REQUIRED CONFIG)

add_executable(memgraph__e2e__server_connection server_connection.cpp)
target_link_libraries(memgraph__e2e__server_connection mgclient::mgclient mg-utils gflags)

add_executable(memgraph__e2e__server_ssl_connection server_ssl_connection.cpp)
target_link_libraries(memgraph__e2e__server_ssl_connection mgclient::mgclient mg-utils gflags)

copy_e2e_files(server workloads.yaml)


function(copy_server_e2e_python_files FILE_NAME)
    copy_e2e_python_files(server ${FILE_NAME})
endfunction()

copy_server_e2e_python_files(common.py)
copy_server_e2e_python_files(test_ssl_hot_reload.py)
copy_server_e2e_python_files(test_ssl_reload_no_ssl.py)
copy_server_e2e_python_files(test_ssl_hot_reload_coordinator.py)
# Copy the entire tls_certs/ directory (self-signed certs used by SSL
# hot-reload tests) to the build dir. Tests resolve paths via SCRIPT_DIR
# / BUILD_DIR + "tls_certs/".
add_custom_target(memgraph__e2e__server__tls_certs_dir ALL
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/tls_certs
    ${CMAKE_CURRENT_BINARY_DIR}/tls_certs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tls_certs)
add_dependencies(memgraph__e2e memgraph__e2e__server__tls_certs_dir)

copy_e2e_python_files_from_parent_folder(server ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(server ".." memgraph.py)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
