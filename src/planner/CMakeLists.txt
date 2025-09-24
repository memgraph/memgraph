add_library(mg-planner)
add_library(mg::planner ALIAS mg-planner)

target_sources(mg-planner
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/planner/core/concepts.hpp
    include/planner/core/constants.hpp
    include/planner/core/eclass.hpp
    include/planner/core/egraph.hpp
    include/planner/core/eids.hpp
    include/planner/core/enode.hpp
    include/planner/core/fwd.hpp
    include/planner/core/processing_context.hpp
    include/planner/core/union_find.hpp

    PRIVATE
    src/core/eclass.cpp
    src/core/enode.cpp
    src/core/egraph.cpp
    src/core/union_find.cpp
)

target_link_libraries(mg-planner PUBLIC absl::flat_hash_map mg-utils lib::rangev3)

if(MG_ENABLE_TESTING)
  add_subdirectory(test)
  add_subdirectory(bench)
endif()
