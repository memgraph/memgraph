add_executable(csv_to_snapshot
  csv_to_snapshot/main.cpp
  # This is just friggin terrible. csv_to_snapshot needs to depend almost on
  # the whole memgraph, just to use TypedValue and BaseEncoder.
  ${memgraph_src_dir}/data_structures/concurrent/skiplist_gc.cpp
  ${memgraph_src_dir}/database/graph_db_accessor.cpp
  ${memgraph_src_dir}/query/typed_value.cpp
  ${memgraph_src_dir}/storage/edge_accessor.cpp
  ${memgraph_src_dir}/storage/locking/record_lock.cpp
  ${memgraph_src_dir}/storage/property_value.cpp
  ${memgraph_src_dir}/storage/record_accessor.cpp
  ${memgraph_src_dir}/storage/vertex_accessor.cpp
  ${memgraph_src_dir}/transactions/transaction.cpp
  )

# Strip the executable in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET csv_to_snapshot POST_BUILD
                     COMMAND strip -s csv_to_snapshot
                     COMMENT Stripping symbols and sections from csv_to_snapshot)
endif()

target_link_libraries(csv_to_snapshot stdc++fs Threads::Threads fmt
                      gflags glog cppitertools)
install(TARGETS csv_to_snapshot
        RUNTIME DESTINATION .)
