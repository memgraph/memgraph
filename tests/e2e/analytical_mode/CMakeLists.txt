function(copy_analytical_mode_e2e_python_files FILE_NAME)
    copy_e2e_python_files(analytical_mode ${FILE_NAME})
endfunction()

copy_analytical_mode_e2e_python_files(common.py)
copy_analytical_mode_e2e_python_files(free_memory.py)

copy_e2e_files(analytical_mode workloads.yaml)
