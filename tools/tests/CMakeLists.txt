include_directories(SYSTEM ${GTEST_INCLUDE_DIR})

add_executable(mg_recovery_check mg_recovery_check.cpp)
target_link_libraries(mg_recovery_check memgraph_lib gtest gtest_main kvstore_dummy_lib)

add_executable(mg_statsd_client statsd/mg_statsd_client.cpp)
target_link_libraries(mg_statsd_client memgraph_lib kvstore_dummy_lib)

# Copy CSV data to CMake build dir
configure_file(csv/comment_nodes.csv csv/comment_nodes.csv COPYONLY)
configure_file(csv/forum_nodes.csv csv/forum_nodes.csv COPYONLY)
configure_file(csv/relationships_0.csv csv/relationships_0.csv COPYONLY)
configure_file(csv/relationships_1.csv csv/relationships_1.csv COPYONLY)
# Copy the actual runner to CMake build dir
configure_file(test_mg_import_csv test_mg_import_csv COPYONLY)

add_test(NAME test_mg_import_csv
         COMMAND test_mg_import_csv
                 --mg-import-csv ../src/mg_import_csv
                 --mg-recovery-check ./mg_recovery_check)
