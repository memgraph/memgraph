set(test_prefix memgraph__stress__)

add_custom_target(memgraph__stress)

function(add_stress_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  # add target to dependencies
  add_dependencies(memgraph__stress ${target_name})
endfunction(add_stress_test)

add_stress_test(long_running.cpp)
target_link_libraries(${test_prefix}long_running mg-communication mg-io mg-utils)
