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
    include/planner/core/eclass.hpp
    include/planner/core/egraph.hpp
    include/planner/core/enode.hpp
    include/planner/core/extractor.hpp
    include/planner/core/fwd.hpp
    include/planner/core/processing_context.hpp

    PRIVATE
    src/core/eclass.cpp
    src/core/enode.cpp
    src/core/egraph.cpp
    src/core/extractor.cpp
)
target_include_directories(mg-planner PUBLIC include)

target_link_libraries(mg-planner PUBLIC absl::flat_hash_map mg-utils range-v3::range-v3)

if(MG_ENABLE_TESTING)
  add_subdirectory(test)
  add_subdirectory(bench)
endif()
