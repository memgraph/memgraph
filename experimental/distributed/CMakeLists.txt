project(distributed)

# set directory variables
set(src_dir ${PROJECT_SOURCE_DIR}/src)
set(libs_dir ${PROJECT_SOURCE_DIR}/libs)

# includes
include_directories(SYSTEM ${libs_dir}/cereal/include)
include_directories(${src_dir})

# library from distributed sources
file(GLOB_RECURSE src_files ${src_dir}/*.cpp)
add_library(distributed_lib STATIC ${src_files})

## distributed Memgraph executable
set(executable_name main)
add_executable(${executable_name} ${PROJECT_SOURCE_DIR}/main.cpp)
target_link_libraries(${executable_name} distributed_lib memgraph_lib)

## dummy distributed Memgraph client
set(executable_name main-client)
add_executable(${executable_name} ${PROJECT_SOURCE_DIR}/main-client.cpp)
target_link_libraries(${executable_name} distributed_lib memgraph_lib)

# tests
enable_testing()
add_subdirectory(tests)

# copy test scripts into the build/ directory (for distributed tests)
configure_file(${PROJECT_SOURCE_DIR}/tests/start_distributed.py
  ${PROJECT_BINARY_DIR}/tests/start_distributed.py COPYONLY)

configure_file(${PROJECT_SOURCE_DIR}/tests/config
  ${PROJECT_BINARY_DIR}/tests/config COPYONLY)

# copy main scripts into build/ directory (for distributed Memgraph)
configure_file(${PROJECT_SOURCE_DIR}/start_main.py
  ${PROJECT_BINARY_DIR}/start_main.py COPYONLY)

configure_file(${PROJECT_SOURCE_DIR}/config
  ${PROJECT_BINARY_DIR}/config COPYONLY)

