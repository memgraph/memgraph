function(copy_load_remote_snapshot_e2e_python_files FILE_NAME)
    copy_e2e_python_files(load_remote_snapshot ${FILE_NAME})
endfunction()

function(copy_load_remote_snapshot_e2e_files FILE_NAME)
    copy_e2e_python_files(load_remote_snapshot ${FILE_NAME})
endfunction()

copy_load_remote_snapshot_e2e_python_files(common.py)
copy_load_remote_snapshot_e2e_python_files(load_remote_snapshot.py)
copy_e2e_files(load_remote_snapshot workloads.yaml)
copy_e2e_files(load_remote_snapshot test_snapshot)
