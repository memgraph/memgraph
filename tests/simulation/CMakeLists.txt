set(test_prefix memgraph__simulation__)

find_package(gflags)

add_custom_target(memgraph__simulation)

function(add_simulation_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} gtest gmock gtest_main)

  # sanitize?
  target_compile_options(${target_name} PRIVATE -fsanitize=address)
  target_link_options(${target_name} PRIVATE -fsanitize=address)

  # register test
  add_test(${target_name} ${exec_name})
  add_dependencies(memgraph__simulation ${target_name})
endfunction(add_simulation_test)

add_simulation_test(future.cpp)
target_link_libraries(${test_prefix}future mg-utils mg-io-v3)

add_simulation_test(basic_request.cpp)
target_link_libraries(${test_prefix}basic_request mg-utils mg-io-v3)
