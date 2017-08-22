project(distributed)

# threading
find_package(Threads REQUIRED)

# set directory variables
set(src_dir ${PROJECT_SOURCE_DIR}/src)
set(libs_dir ${PROJECT_SOURCE_DIR}/libs)

# includes
include_directories(${libs_dir}/cereal/include)
include_directories(${src_dir})

# totally hacked, no idea why I need to include these again
# TODO: ask teon
include_directories(${CMAKE_SOURCE_DIR}/libs/fmt)
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(SYSTEM ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/libs)
# needed to include configured files (plan_compiler_flags.hpp)
set(generated_headers_dir ${CMAKE_BINARY_DIR}/generated_headers)
include_directories(${generated_headers_dir})
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/libs/glog/include)
include_directories(SYSTEM  ${CMAKE_BINARY_DIR}/libs/gflags/include)

# library from distributed sources
file(GLOB_RECURSE src_files ${src_dir}/*.cpp)
add_library(distributed_lib STATIC ${src_files})

## executable
#### HACK: there is temporarily no working main file as the API is changing
# set(executable_name distributed)
# add_executable(${executable_name} ${PROJECT_SOURCE_DIR}/main.cpp)
# target_link_libraries(${executable_name} distributed_lib)
# target_link_libraries(${executable_name} memgraph_lib)
# target_link_libraries(${executable_name} ${MEMGRAPH_ALL_LIBS})

# tests
add_subdirectory(${PROJECT_SOURCE_DIR}/tests)

# copy test scripts into the build/ directory
configure_file(${PROJECT_SOURCE_DIR}/tests/start_distributed.py
  ${PROJECT_BINARY_DIR}/tests/start_distributed.py COPYONLY)

configure_file(${PROJECT_SOURCE_DIR}/tests/config
  ${PROJECT_BINARY_DIR}/tests/config COPYONLY)

