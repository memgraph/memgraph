add_library(mg-versioning STATIC)
add_library(mg::versioning ALIAS mg-versioning)
target_sources(mg-versioning
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ..
        FILES
        branch_change_filter.hpp
        branch_engine.hpp
        branch_info.hpp
        branch_ledger.hpp
        branch_recovery.hpp
        edge_overlay_index.hpp
        gate.hpp
        overlay_state.hpp
        overlay_mark_deleted.hpp
        overlay_index.hpp
        overlay_merge.hpp
        version_store.hpp

        PRIVATE
        branch_engine.cpp
        branch_info.cpp
        branch_ledger.cpp
        branch_recovery.cpp
        gate.cpp
        version_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-utils mg-kvstore mg-storage-v2 mg-metrics mg-memory-utils)
