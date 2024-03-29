function(copy_batched_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(batched_procedures ${FILE_NAME})
endfunction()

copy_batched_procedures_e2e_python_files(common.py)
copy_batched_procedures_e2e_python_files(conftest.py)
copy_batched_procedures_e2e_python_files(simple_read.py)

add_subdirectory(procedures)

copy_e2e_files(batched_procedures workloads.yaml)
