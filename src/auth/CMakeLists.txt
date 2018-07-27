set(auth_src_files
    auth.cpp
    crypto.cpp
    models.cpp)

add_library(mg-auth STATIC ${auth_src_files})
target_link_libraries(mg-auth json libbcrypt)
target_link_libraries(mg-auth mg-utils)
