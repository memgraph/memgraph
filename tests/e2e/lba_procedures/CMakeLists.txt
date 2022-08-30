function(copy_lba_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(lba_procedures ${FILE_NAME})
endfunction()

copy_lba_procedures_e2e_python_files(common.py)
copy_lba_procedures_e2e_python_files(read_query_modules.py)

add_subdirectory(procedures)
