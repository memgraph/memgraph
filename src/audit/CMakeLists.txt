set(audit_src_files log.cpp)

add_library(mg-audit STATIC ${audit_src_files})
target_link_libraries(mg-audit json glog gflags fmt)
target_link_libraries(mg-audit mg-utils mg-storage-v2)
