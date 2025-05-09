function(copy_scheduler_e2e_python_files FILE_NAME)
    copy_e2e_python_files(scheduler ${FILE_NAME})
endfunction()

copy_scheduler_e2e_python_files(priority_queue.py)

copy_e2e_files(scheduler workloads.yaml)
