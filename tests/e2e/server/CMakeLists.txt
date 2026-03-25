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
copy_e2e_files(server ssl_reload_test.crt)
copy_e2e_files(server ssl_reload_test.key)
