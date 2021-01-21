set(test_prefix louvain__unit__)

add_custom_target(louvain__unit)

add_library(louvain-test STATIC utils.cpp)
set_target_properties(louvain-test PROPERTIES POSITION_INDEPENDENT_CODE ON)

function(add_unit_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  # TODO: this is a temporary workaround the test build warnings
  target_compile_options(${target_name} PRIVATE -Wno-comment -Wno-sign-compare
                                                -Wno-unused-variable)
  target_link_libraries(${target_name} spdlog gflags gtest gtest_main Threads::Threads
                                       louvain-core louvain-test)
  # register test
  add_test(${target_name} ${exec_name})
  # add to unit target
  add_dependencies(louvain__unit ${target_name})
endfunction(add_unit_test)

add_unit_test(graph.cpp)
