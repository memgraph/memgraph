set(target_name memgraph__e2e__replication_indexes)

add_executable(${target_name} main.cpp)
target_link_libraries(${target_name} glog gflags Threads::Threads)
