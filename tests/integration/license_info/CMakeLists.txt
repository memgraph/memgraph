set(target_name memgraph__integration__license_info)
set(client_target_name ${target_name}__client)

add_executable(${client_target_name} client.cpp)
set_target_properties(${client_target_name} PROPERTIES OUTPUT_NAME client)
target_link_libraries(${client_target_name} mg-requests mg-license mg-utils)

# Rewrite each test binary's versioned libpython DT_NEEDED to the abi3 SONAME
# (libpython3.so), like memgraph and the unit tests, so they keep loading when the
# runtime Python is swapped. No-op for binaries without a libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
