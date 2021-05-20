add_executable(memgraph__e2e__memory__control memory_control.cpp)
target_link_libraries(memgraph__e2e__memory__control gflags mgclient mg-utils mg-io Threads::Threads)
