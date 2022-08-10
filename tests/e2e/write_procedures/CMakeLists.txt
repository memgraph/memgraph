function(copy_write_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(write_procedures ${FILE_NAME})
endfunction()

copy_write_procedures_e2e_python_files(common.py)
copy_write_procedures_e2e_python_files(conftest.py)
copy_write_procedures_e2e_python_files(simple_write.py)
copy_write_procedures_e2e_python_files(lba_procedures.py)

add_subdirectory(procedures)
