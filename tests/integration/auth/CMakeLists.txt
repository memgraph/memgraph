set(target_name memgraph__integration__auth)
set(checker_target_name ${target_name}__checker)
set(tester_target_name ${target_name}__tester)

add_executable(${checker_target_name} checker.cpp)
set_target_properties(${checker_target_name} PROPERTIES OUTPUT_NAME checker)
target_link_libraries(${checker_target_name} mg-communication)

add_executable(${tester_target_name} tester.cpp)
set_target_properties(${tester_target_name} PROPERTIES OUTPUT_NAME tester)
target_link_libraries(${tester_target_name} mg-communication)
