set(audit_src_files log.cpp)

find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

add_library(mg-audit STATIC ${audit_src_files})
target_link_libraries(mg-audit json gflags fmt::fmt)
target_link_libraries(mg-audit mg-utils mg-storage-v2)
