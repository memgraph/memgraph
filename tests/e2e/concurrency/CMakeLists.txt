function(copy_concurrency_e2e_python_files FILE_NAME)
    copy_e2e_python_files(concurrency ${FILE_NAME})
endfunction()

copy_concurrency_e2e_python_files(common.py)
copy_concurrency_e2e_python_files(concurrency.py)
