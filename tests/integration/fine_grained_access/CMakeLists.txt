set(target_name memgraph__integration__fine_grained_access)
set(tester_target_name ${target_name}__tester)
set(filtering_target_name ${target_name}__filtering)

add_executable(${tester_target_name} tester.cpp)
set_target_properties(${tester_target_name} PROPERTIES OUTPUT_NAME tester)
target_link_libraries(${tester_target_name} mg-communication)

add_executable(${filtering_target_name} filtering.cpp)
set_target_properties(${filtering_target_name} PROPERTIES OUTPUT_NAME filtering)
target_link_libraries(${filtering_target_name} mg-communication)
