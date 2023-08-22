set(target_name memgraph__integration__executor)
set(tester_target_name ${target_name}__tester)
set(executor_target_name ${target_name}__executor)

add_executable(${tester_target_name} tester.cpp)
set_target_properties(${tester_target_name} PROPERTIES OUTPUT_NAME tester)
target_link_libraries(${tester_target_name} mg-communication)

add_executable(${executor_target_name} executor.cpp)
set_target_properties(${executor_target_name} PROPERTIES OUTPUT_NAME executor)
target_link_libraries(${executor_target_name} mg-communication)
