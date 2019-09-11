set(auth_src_files
    auth.cpp
    crypto.cpp
    models.cpp
    module.cpp)

find_package(Seccomp REQUIRED)

add_library(mg-auth STATIC ${auth_src_files})
target_link_libraries(mg-auth json libbcrypt glog gflags fmt)
target_link_libraries(mg-auth mg-utils)

target_link_libraries(mg-auth ${Seccomp_LIBRARIES})
target_include_directories(mg-auth SYSTEM PRIVATE ${Seccomp_INCLUDE_DIRS})
