set(target_name memgraph__integration__license_info)
set(client_target_name ${target_name}__client)

add_executable(${client_target_name} client.cpp)
set_target_properties(${client_target_name} PROPERTIES OUTPUT_NAME client)
target_link_libraries(${client_target_name} mg-requests mg-license mg-utils)
