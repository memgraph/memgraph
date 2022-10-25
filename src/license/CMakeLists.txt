set(license_src_files
    license.cpp)
add_library(mg-license STATIC ${license_src_files})

target_link_libraries(mg-license mg-settings mg-utils)
