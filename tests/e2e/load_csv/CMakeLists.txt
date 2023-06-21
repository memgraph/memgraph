function(copy_load_csv_e2e_python_files FILE_NAME)
    copy_e2e_python_files(load_csv ${FILE_NAME})
endfunction()

function(copy_load_csv_e2e_files FILE_NAME)
    copy_e2e_python_files(load_csv ${FILE_NAME})
endfunction()

copy_load_csv_e2e_python_files(load_csv.py)
copy_load_csv_e2e_files(simple.csv)
