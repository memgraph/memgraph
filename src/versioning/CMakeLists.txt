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
        edge_stub_index.hpp
        gate.hpp
        index_key_parts.hpp
        name.hpp
        stub_state.hpp
        stub_index.hpp
        stub_merge.hpp
        version_store.hpp

        PRIVATE
        branch_engine.cpp
        branch_ledger.cpp
        branch_recovery.cpp
        gate.cpp
        name.cpp
        version_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-utils mg-storage-v2 mg-metrics mg-memory-utils)
