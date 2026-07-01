set(target_name memgraph__integration__executor)
set(tester_target_name ${target_name}__tester)
set(flag_tester_target_name ${target_name}__flag_tester)
set(executor_target_name ${target_name}__executor)
set(config_checker_target_name ${target_name}__config_checker)

add_executable(${tester_target_name} tester.cpp)
set_target_properties(${tester_target_name} PROPERTIES OUTPUT_NAME tester)
target_link_libraries(${tester_target_name} mg-communication)

add_executable(${flag_tester_target_name} flag_tester.cpp)
set_target_properties(${flag_tester_target_name} PROPERTIES OUTPUT_NAME flag_tester)
target_link_libraries(${flag_tester_target_name} mg-communication)

add_executable(${executor_target_name} executor.cpp)
set_target_properties(${executor_target_name} PROPERTIES OUTPUT_NAME executor)
target_link_libraries(${executor_target_name} mg-communication)

add_executable(${config_checker_target_name} config_checker.cpp)
set_target_properties(${config_checker_target_name} PROPERTIES OUTPUT_NAME config_checker)
target_link_libraries(${config_checker_target_name} mg-communication)
