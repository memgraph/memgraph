set(test_prefix memgraph__manual__)

add_custom_target(memgraph__manual)

function(add_manual_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
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
target_link_libraries(${test_prefix}bolt_client memgraph_lib kvstore_dummy_lib)

add_manual_test(card_fraud_generate_snapshot.cpp)
target_link_libraries(${test_prefix}card_fraud_generate_snapshot memgraph_lib kvstore_dummy_lib)

add_manual_test(card_fraud_local.cpp)
target_link_libraries(${test_prefix}card_fraud_local memgraph_lib kvstore_dummy_lib)

add_manual_test(distributed_repl.cpp)
target_link_libraries(${test_prefix}distributed_repl memgraph_lib kvstore_dummy_lib)

add_manual_test(endinan.cpp)

add_manual_test(generate_snapshot.cpp)
target_link_libraries(${test_prefix}generate_snapshot memgraph_lib kvstore_dummy_lib)

add_manual_test(graph_500_generate_snapshot.cpp)
target_link_libraries(${test_prefix}graph_500_generate_snapshot memgraph_lib kvstore_dummy_lib)

add_manual_test(query_hash.cpp)
target_link_libraries(${test_prefix}query_hash memgraph_lib kvstore_dummy_lib)

add_manual_test(query_planner.cpp)
target_link_libraries(${test_prefix}query_planner memgraph_lib kvstore_dummy_lib)

add_manual_test(raft_rpc.cpp)
target_link_libraries(${test_prefix}raft_rpc memgraph_lib kvstore_dummy_lib)

add_manual_test(repl.cpp)
target_link_libraries(${test_prefix}repl memgraph_lib kvstore_dummy_lib)

add_manual_test(single_query.cpp)
target_link_libraries(${test_prefix}single_query memgraph_lib kvstore_dummy_lib)

add_manual_test(sl_position_and_count.cpp)
target_link_libraries(${test_prefix}sl_position_and_count memgraph_lib kvstore_dummy_lib)

add_manual_test(stripped_timing.cpp)
target_link_libraries(${test_prefix}stripped_timing memgraph_lib kvstore_dummy_lib)

add_manual_test(xorshift.cpp)
target_link_libraries(${test_prefix}xorshift mg-utils)

