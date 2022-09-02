function(copy_lba_procedures_e2e_python_files FILE_NAME)
    copy_e2e_python_files(lba_procedures ${FILE_NAME})
endfunction()

copy_lba_procedures_e2e_python_files(common.py)
copy_lba_procedures_e2e_python_files(lba_procedures.py)
copy_lba_procedures_e2e_python_files(show_privileges.py)
copy_lba_procedures_e2e_python_files(read_permission_queries.py)

add_subdirectory(procedures)
