function(copy_periodic_commit_e2e_python_files FILE_NAME)
    copy_e2e_python_files(periodic_commit ${FILE_NAME})
endfunction()

copy_periodic_commit_e2e_python_files(common.py)
copy_periodic_commit_e2e_python_files(periodic_commit.py)

copy_e2e_files(periodic_commit workloads.yaml)
