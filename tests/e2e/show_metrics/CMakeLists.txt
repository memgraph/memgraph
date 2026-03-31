function(copy_show_metrics_e2e_python_files FILE_NAME)
    copy_e2e_python_files(show_metrics ${FILE_NAME})
endfunction()

copy_show_metrics_e2e_python_files(common.py)
copy_show_metrics_e2e_python_files(show_metrics.py)
copy_show_metrics_e2e_python_files(show_metrics_snapshot.py)
copy_show_metrics_e2e_python_files(show_metrics_http.py)
copy_show_metrics_e2e_python_files(show_metrics_recovery.py)
copy_e2e_python_files_from_parent_folder(show_metrics ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(show_metrics ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(show_metrics ".." mg_utils.py)

copy_e2e_files(show_metrics workloads.yaml)
