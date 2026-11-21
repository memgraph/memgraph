add_library(mg-versioning STATIC)
add_library(mg::versioning ALIAS mg-versioning)
target_sources(mg-versioning
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ..
        FILES
        branch_info.hpp
        branch_ledger.hpp
        gate.hpp

        PRIVATE
        branch_info.cpp
        branch_ledger.cpp
        gate.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-utils mg-kvstore mg-storage-v2)
