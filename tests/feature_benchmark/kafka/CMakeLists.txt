set(target_name memgraph__feature_benchmark__kafka)

set(benchmark_target_name ${target_name}__benchmark)
add_executable(${benchmark_target_name} benchmark.cpp)
set_target_properties(${benchmark_target_name} PROPERTIES OUTPUT_NAME benchmark)
target_link_libraries(${benchmark_target_name} memgraph_lib kvstore_lib)

# Copy kafka.py to the feature integration kafka folder
configure_file(${PROJECT_SOURCE_DIR}/src/integrations/kafka/kafka.py ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
