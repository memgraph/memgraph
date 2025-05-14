function(copy_garbage_collection_e2e_python_files FILE_NAME)
    copy_e2e_python_files(garbage_collection ${FILE_NAME})
endfunction()

copy_garbage_collection_e2e_python_files(common.py)
copy_garbage_collection_e2e_python_files(gc_on_obsolete_indexes.py)

add_executable(memgraph__e2e__garbage_collection periodic.cpp)
target_link_libraries(memgraph__e2e__garbage_collection mg::storage)

copy_e2e_files(garbage_collection workloads.yaml)
