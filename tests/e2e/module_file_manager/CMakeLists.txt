find_package(gflags REQUIRED)

add_executable(memgraph__e2e__module_file_manager module_file_manager.cpp)
target_link_libraries(memgraph__e2e__module_file_manager gflags mgclient mg-utils mg-io Threads::Threads)
