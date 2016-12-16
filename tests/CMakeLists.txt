cmake_minimum_required(VERSION 3.1)

project(${project_name}_tests)

set(src_dir ${CMAKE_SOURCE_DIR}/src)

# copy unit test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests)

# TODO: test logic here T190
