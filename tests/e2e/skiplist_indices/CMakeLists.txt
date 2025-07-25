function(copy_skiplist_indices_e2e_python_files FILE_NAME)
    copy_e2e_python_files(skiplist_indices ${FILE_NAME})
endfunction()

copy_skiplist_indices_e2e_python_files(common.py)
copy_skiplist_indices_e2e_python_files(edge_indices.py)
copy_skiplist_indices_e2e_python_files(plan_invalidation.py)

copy_e2e_files(skiplist_indices workloads.yaml)
