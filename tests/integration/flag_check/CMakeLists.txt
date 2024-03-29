set(target_name memgraph__integration__flag_check)
set(tester_target_name ${target_name}__tester)
set(flag_check_target_name ${target_name}__flag_check)

add_executable(${tester_target_name} tester.cpp)
set_target_properties(${tester_target_name} PROPERTIES OUTPUT_NAME tester)
target_link_libraries(${tester_target_name} mg-communication)

add_executable(${flag_check_target_name} flag_check.cpp)
set_target_properties(${flag_check_target_name} PROPERTIES OUTPUT_NAME flag_check)
target_link_libraries(${flag_check_target_name} mg-communication)
