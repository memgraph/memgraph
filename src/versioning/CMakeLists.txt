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
        gate.hpp
        main_constraints.hpp
        name.hpp
        stub_index.hpp
        stub_properties.hpp
        version_store.hpp
        vertex_overlay.hpp
        vertex_overlay_store.hpp

        PRIVATE
        branch_engine.cpp
        gate.cpp
        name.cpp
        version_store.cpp
        vertex_overlay_store.cpp
)

target_link_libraries(mg-versioning PUBLIC mg-utils mg-storage-v2 mg-metrics)
