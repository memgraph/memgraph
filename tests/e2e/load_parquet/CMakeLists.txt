function(copy_load_parquet_e2e_python_files FILE_NAME)
    copy_e2e_python_files(load_parquet ${FILE_NAME})
endfunction()

function(copy_load_parquet_e2e_files FILE_NAME)
    copy_e2e_python_files(load_parquet ${FILE_NAME})
endfunction()

copy_load_parquet_e2e_python_files(common.py)
copy_load_parquet_e2e_python_files(load_parquet.py)
copy_load_parquet_e2e_files(edges_100.parquet)
copy_load_parquet_e2e_files(edges_with_nulls.parquet)
copy_load_parquet_e2e_files(nodes_100.parquet)
copy_load_parquet_e2e_files(nodes_with_nulls.parquet)
copy_load_parquet_e2e_files(nodes_numeric.parquet)
copy_load_parquet_e2e_files(nodes_temporal.parquet)
copy_load_parquet_e2e_files(nodes_binary.parquet)

copy_e2e_files(load_parquet workloads.yaml)
