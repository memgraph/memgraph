set(auth_src_files
    auth.cpp
    crypto.cpp
    models.cpp)

find_package(Ldap REQUIRED)

add_library(mg-auth STATIC ${auth_src_files})
target_link_libraries(mg-auth json libbcrypt glog gflags fmt ldap)
target_link_libraries(mg-auth mg-utils)
