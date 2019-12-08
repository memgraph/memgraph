set(test_prefix memgraph__manual__)

add_custom_target(memgraph__manual)

function(add_manual_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp} ${ARGN})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  add_dependencies(memgraph__manual ${target_name})
endfunction(add_manual_test)

add_manual_test(antlr_parser.cpp)
target_link_libraries(${test_prefix}antlr_parser antlr_opencypher_parser_lib)

add_manual_test(antlr_sigsegv.cpp)
target_link_libraries(${test_prefix}antlr_sigsegv gtest gtest_main
                      antlr_opencypher_parser_lib mg-utils)

add_manual_test(antlr_tree_pretty_print.cpp)
target_link_libraries(${test_prefix}antlr_tree_pretty_print antlr_opencypher_parser_lib)

add_manual_test(binomial.cpp)
target_link_libraries(${test_prefix}binomial mg-utils)

add_manual_test(bolt_client.cpp)
target_link_libraries(${test_prefix}bolt_client mg-communication)

add_manual_test(ha_client.cpp)
target_link_libraries(${test_prefix}ha_client mg-utils mg-communication)

add_manual_test(ha_proxy.cpp)
target_include_directories(${test_prefix}ha_proxy PRIVATE ${CMAKE_BINARY_DIR}/src)
target_link_libraries(${test_prefix}ha_proxy mg-utils mg-communication)

add_manual_test(kvstore_console.cpp)
target_link_libraries(${test_prefix}kvstore_console kvstore_lib gflags glog)

add_manual_test(query_hash.cpp)
target_link_libraries(${test_prefix}query_hash mg-single-node-v2 mg-auth kvstore_dummy_lib)

add_manual_test(query_planner.cpp interactive_planning.cpp)
target_link_libraries(${test_prefix}query_planner mg-single-node-v2 mg-auth kvstore_dummy_lib)
if (READLINE_FOUND)
  target_link_libraries(${test_prefix}query_planner readline)
endif()

add_manual_test(expression_pretty_printer.cpp)
target_link_libraries(${test_prefix}expression_pretty_printer mg-single-node-v2 mg-auth kvstore_dummy_lib)

add_manual_test(single_query.cpp)
target_link_libraries(${test_prefix}single_query mg-single-node-v2 mg-auth kvstore_dummy_lib)

add_manual_test(sl_position_and_count.cpp)
target_link_libraries(${test_prefix}sl_position_and_count mg-single-node kvstore_dummy_lib)

add_manual_test(snapshot_explorer.cpp)
target_link_libraries(${test_prefix}snapshot_explorer mg-single-node kvstore_dummy_lib)

add_manual_test(stripped_timing.cpp)
target_link_libraries(${test_prefix}stripped_timing mg-single-node-v2 mg-auth kvstore_dummy_lib)

add_manual_test(ssl_client.cpp)
target_link_libraries(${test_prefix}ssl_client mg-communication)

add_manual_test(ssl_server.cpp)
target_link_libraries(${test_prefix}ssl_server mg-communication)

add_manual_test(wal_explorer.cpp)
target_link_libraries(${test_prefix}wal_explorer mg-single-node kvstore_dummy_lib)

add_manual_test(xorshift.cpp)
target_link_libraries(${test_prefix}xorshift mg-utils)

