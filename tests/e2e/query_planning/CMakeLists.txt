function(copy_query_planning_e2e_python_files FILE_NAME)
    copy_e2e_python_files(query_planning ${FILE_NAME})
endfunction()

copy_query_planning_e2e_python_files(common.py)
copy_query_planning_e2e_python_files(query_planning_cartesian.py)
