add_library(mg-planner)
add_library(mg::planner ALIAS mg-planner)

target_sources(mg-planner
    PUBLIC
    FILE_SET CXX_MODULES FILES
    strong_type.cppm
    concepts.cppm
    constants.cppm
    eids.cppm
    union_find.cppm

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    # New organized headers by concept
    # egraph/ - Core e-graph data structure
    include/planner/egraph/eclass.hpp
    include/planner/egraph/egraph.hpp
    include/planner/egraph/enode.hpp
    include/planner/egraph/fwd.hpp
    include/planner/egraph/processing_context.hpp

    # pattern/ - Pattern definition and matching
    include/planner/pattern/match.hpp
    include/planner/pattern/matcher.hpp
    include/planner/pattern/pattern.hpp

    # rewrite/ - Rewriting/saturation engine
    include/planner/rewrite/join.hpp
    include/planner/rewrite/rewriter.hpp
    include/planner/rewrite/rule.hpp

    # extract/ - Cost extraction
    include/planner/extract/extractor.hpp

    PRIVATE
    # egraph/
    src/egraph/eclass.cpp
    src/egraph/egraph.cpp
    src/egraph/enode.cpp
    # pattern/
    src/pattern/matcher.cpp
    src/pattern/pattern.cpp
    # rewrite/
    src/rewrite/rewriter.cpp
    # extract/
    src/extract/extractor.cpp
)
target_include_directories(mg-planner PUBLIC include)

target_link_libraries(mg-planner PUBLIC absl::flat_hash_map mg-utils range-v3::range-v3)

if(MG_ENABLE_TESTING)
  add_subdirectory(test)
  add_subdirectory(bench)
endif()
