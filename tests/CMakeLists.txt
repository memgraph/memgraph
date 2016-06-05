cmake_minimum_required(VERSION 3.1)

project(memgraph_tests)

# setup dependencies
find_package(Threads REQUIRED)

# Catch (C++ Automated Test Cases in Headers) dependency
ExternalProject_Add(
    Catch
    GIT_REPOSITORY "https://github.com/philsquared/Catch.git"
    GIT_TAG "master"
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/libs/Catch"
	CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    TEST_COMMAND ""
)
ExternalProject_Get_Property(Catch source_dir)
set(catch_source_dir ${source_dir})

include_directories(${catch_source_dir}/include)

# find tests
file(GLOB_RECURSE test_files ${CMAKE_HOME_DIRECTORY}/tests/*.cpp)
set(tests "")
foreach(test_file ${test_files})
    get_filename_component(test_name ${test_file} NAME_WE)
    list(APPEND tests ${test_name})
endforeach()
MESSAGE(STATUS "Available tests are: ${tests}")

# copy test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data DESTINATION ${CMAKE_BINARY_DIR}/tests)

# build tests
foreach(test ${tests})
    add_executable(${test} ${test}.cpp)
    # TODO: separate dependencies
    target_link_libraries(${test} stdc++fs)
    target_link_libraries(${test} cypher_lib)
    target_link_libraries(${test} Threads::Threads)
    add_test(NAME ${test} COMMAND ${test})
    set_property(TARGET ${test} PROPERTY CXX_STANDARD 14)
endforeach()
