function(transaction_rollback_e2e_python_files FILE_NAME)
    copy_e2e_python_files(transaction_rollback ${FILE_NAME})
endfunction()

transaction_rollback_e2e_python_files(common.py)
transaction_rollback_e2e_python_files(conftest.py)
transaction_rollback_e2e_python_files(transaction.py)

add_subdirectory(procedures)

copy_e2e_files(transaction_rollback workloads.yaml)
