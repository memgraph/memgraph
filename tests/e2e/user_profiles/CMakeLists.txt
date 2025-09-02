function(copy_user_profiles_e2e_python_files FILE_NAME)
    copy_e2e_python_files(user_profiles ${FILE_NAME})
endfunction()

copy_user_profiles_e2e_python_files(user_profiles_e2e.py)
copy_user_profiles_e2e_python_files(concurrent_user_profiles_e2e.py)

copy_e2e_files(user_profiles workloads.yaml)
