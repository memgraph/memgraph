include_directories(SYSTEM ${GTEST_INCLUDE_DIR})

add_executable(mg_recovery_check
  mg_recovery_check.cpp
  ${memgraph_src_dir}/communication/bolt/v1/decoder/decoded_value.cpp
  ${memgraph_src_dir}/data_structures/concurrent/skiplist_gc.cpp
  ${memgraph_src_dir}/database/graph_db.cpp
  ${memgraph_src_dir}/database/graph_db_accessor.cpp
  ${memgraph_src_dir}/durability/recovery.cpp
  ${memgraph_src_dir}/durability/snapshooter.cpp
  ${memgraph_src_dir}/query/typed_value.cpp
  ${memgraph_src_dir}/storage/edge_accessor.cpp
  ${memgraph_src_dir}/storage/locking/record_lock.cpp
  ${memgraph_src_dir}/storage/property_value.cpp
  ${memgraph_src_dir}/storage/record_accessor.cpp
  ${memgraph_src_dir}/storage/vertex_accessor.cpp
  ${memgraph_src_dir}/transactions/transaction.cpp
  )

target_link_libraries(mg_recovery_check stdc++fs Threads::Threads fmt glog
                      gflags cppitertools)
target_link_libraries(mg_recovery_check gtest gtest_main)

# Copy CSV data to CMake build dir
configure_file(csv/comment_nodes.csv csv/comment_nodes.csv COPYONLY)
configure_file(csv/forum_nodes.csv csv/forum_nodes.csv COPYONLY)
configure_file(csv/relationships.csv csv/relationships.csv COPYONLY)
# Copy the actual runner to CMake build dir
configure_file(test_mg_import_csv test_mg_import_csv COPYONLY)

add_test(NAME test_mg_import_csv
         COMMAND test_mg_import_csv
                 --mg-import-csv ../src/mg_import_csv
                 --mg-recovery-check ./mg_recovery_check)
