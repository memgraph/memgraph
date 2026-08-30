function(copy_value_relations_e2e_python_files FILE_NAME)
    copy_e2e_python_files(value_relations ${FILE_NAME})
endfunction()

copy_value_relations_e2e_python_files(common.py)
copy_value_relations_e2e_python_files(index_agreement.py)

copy_e2e_files(value_relations workloads.yaml)
