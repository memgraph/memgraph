set(test_prefix memgraph__mgbench__)

add_executable(${test_prefix}client client.cpp)
set_target_properties(${test_prefix}client PROPERTIES OUTPUT_NAME client)
target_link_libraries(${test_prefix}client mg-communication json)
