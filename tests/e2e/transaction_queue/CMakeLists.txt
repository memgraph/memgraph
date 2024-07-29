function(copy_query_modules_reloading_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(transaction_queue ${FILE_NAME})
endfunction()

copy_query_modules_reloading_procedures_e2e_python_files(common.py)
copy_query_modules_reloading_procedures_e2e_python_files(test_transaction_queue.py)

add_subdirectory(procedures)

copy_e2e_files(transaction_queue workloads.yaml)
copy_e2e_python_files_from_parent_folder(transaction_queue ".." mg_utils.py)
