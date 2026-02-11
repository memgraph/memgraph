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
    enode.cppm
    eclass.cppm
    egraph.cppm

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    # pattern/ - Pattern definition and matching
    include/planner/pattern/match.hpp
    include/planner/pattern/match_index.hpp
    include/planner/pattern/match_storage.hpp
    include/planner/pattern/pattern.hpp

    # pattern/vm/ - VM-based pattern matching
    include/planner/pattern/vm/bytecode_validator.hpp
    include/planner/pattern/vm/compiled_pattern.hpp
    include/planner/pattern/vm/compiler.hpp
    include/planner/pattern/vm/executor.hpp
    include/planner/pattern/vm/instruction.hpp
    include/planner/pattern/vm/state.hpp
    include/planner/pattern/vm/tracer.hpp
    include/planner/pattern/vm/types.hpp

    # rewrite/ - Rewriting/saturation engine
    include/planner/rewrite/rewriter.hpp
    include/planner/rewrite/rule.hpp
    include/planner/rewrite/rule_context.hpp
    include/planner/rewrite/rule_set.hpp

    # extract/ - Cost extraction
    include/planner/extract/extractor.hpp

    PRIVATE
    # egraph/ (module implementation units)
    src/egraph/eclass.cpp
    src/egraph/egraph.cpp
    src/egraph/enode.cpp
    # pattern/
    src/pattern/match.cpp
    src/pattern/match_index.cpp
    src/pattern/match_storage.cpp
    src/pattern/pattern.cpp
    src/pattern/types.cpp
    # pattern/vm/
    src/pattern/vm/compiled_pattern.cpp
    src/pattern/vm/compiler.cpp
    src/pattern/vm/executor.cpp
    src/pattern/vm/instruction.cpp
    src/pattern/vm/state.cpp
    src/pattern/vm/tracer.cpp
    src/pattern/vm/types.cpp
    # rewrite/
    src/rewrite/rewriter.cpp
    src/rewrite/rule.cpp
    src/rewrite/rule_context.cpp
    src/rewrite/rule_set.cpp
    # extract/
    src/extract/extractor.cpp
)
target_link_libraries(mg-planner PUBLIC absl::flat_hash_map mg-utils range-v3::range-v3)

if(MG_ENABLE_TESTING)
  add_subdirectory(test)
  add_subdirectory(bench)
  add_subdirectory(fuzz)
endif()
