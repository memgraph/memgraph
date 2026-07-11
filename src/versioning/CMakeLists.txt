add_library(mg-versioning STATIC)
add_library(mg::versioning ALIAS mg-versioning)
target_sources(mg-versioning
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ..
        FILES
        branch_log.hpp
        gate.hpp
        name.hpp
        version_store.hpp

        PRIVATE
        branch_log.cpp
        gate.cpp
        name.cpp
        version_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-kvstore mg-utils mg-storage-v2)
