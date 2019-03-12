set(target_name memgraph__feature_benchmark__ha__write)

set(benchmark_target_name ${target_name}__benchmark)
add_executable(${benchmark_target_name} benchmark.cpp)
set_target_properties(${benchmark_target_name} PROPERTIES OUTPUT_NAME benchmark)
target_link_libraries(${benchmark_target_name} mg-utils mg-communication)
