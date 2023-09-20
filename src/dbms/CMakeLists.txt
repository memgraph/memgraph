
add_library(mg-dbms STATIC database.cpp)
target_link_libraries(mg-dbms mg-utils mg-storage-v2 mg-query)
