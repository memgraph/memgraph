function(copy_show_metrics_e2e_python_files FILE_NAME)
    copy_e2e_python_files(show_metrics ${FILE_NAME})
endfunction()

copy_show_metrics_e2e_python_files(common.py)
copy_show_metrics_e2e_python_files(show_metrics.py)

copy_e2e_files(show_metrics workloads.yaml)
