find_package(Threads REQUIRED)
include_directories(${BENCHMARK_INCLUDE_DIR})
include_directories(${GTEST_INCLUDE_DIR})

# get all cpp abs file names recursively starting from current directory
file(GLOB poc_cpps *.cpp)
message(STATUS "Available poc cpp files are: ${poc_cpps}")

include_directories(${CMAKE_SOURCE_DIR}/poc)

# for each cpp file build binary
foreach(poc_cpp ${poc_cpps})

    # get exec name (remove extension from the abs path)
    get_filename_component(exec_name ${poc_cpp} NAME_WE)

    set(target_name memgraph_poc_${exec_name})

    # build exe file
    add_executable(${target_name} ${poc_cpp})
    set_property(TARGET ${target_name} PROPERTY CXX_STANDARD ${cxx_standard})

    # OUTPUT_NAME sets the real name of a target when it is built and can be
    # used to help create two targets of the same name even though CMake
    # requires unique logical target names
    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})

    # link libraries
    target_link_libraries(${target_name} memgraph_lib)
    target_link_libraries(${target_name} gtest gtest_main)
    # google-benchmark requires threads
    target_link_libraries(${target_name} benchmark Threads::Threads)

endforeach()
