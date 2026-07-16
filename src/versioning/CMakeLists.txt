add_library(mg-versioning STATIC)
add_library(mg::versioning ALIAS mg-versioning)
target_sources(mg-versioning
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ..
        FILES
        branch_change_filter.hpp
        branch_change_kind.hpp
        branch_engine.hpp
        branch_log.hpp
        branch_overlay.hpp
        branch_reconstruction.hpp
        gate.hpp
        merge.hpp
        name.hpp
        version_store.hpp

        PRIVATE
        branch_engine.cpp
        branch_log.cpp
        branch_overlay.cpp
        branch_reconstruction.cpp
        gate.cpp
        merge.cpp
        name.cpp
        version_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-kvstore mg-utils mg-storage-v2 mg-metrics)
