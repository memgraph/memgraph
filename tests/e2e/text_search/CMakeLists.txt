function(copy_text_search_e2e_python_files FILE_NAME)
    copy_e2e_python_files(text_search ${FILE_NAME})
endfunction()

copy_text_search_e2e_python_files(common.py)
copy_text_search_e2e_python_files(test_text_search.py)
copy_text_search_e2e_python_files(test_text_search_disabled.py)
