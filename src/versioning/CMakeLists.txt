add_library(mg-versioning STATIC)
add_library(mg::versioning ALIAS mg-versioning)
target_sources(mg-versioning
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ..
        FILES
        branch_change_filter.hpp
        branch_info.hpp
        branch_ledger.hpp
        edge_stub_index.hpp
        gate.hpp
        name.hpp
        reserve_for_append.hpp
        reserved_guard_mapper.hpp
        stub_state.hpp
        stub_index.hpp
        version_store.hpp

        PRIVATE
        branch_ledger.cpp
        gate.cpp
        name.cpp
        reserved_guard_mapper.cpp
        version_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-utils mg-storage-v2 mg-metrics)
