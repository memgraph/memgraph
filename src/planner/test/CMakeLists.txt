add_unit_test(planner
    CUSTOM_MAIN
    SOURCES
        main.cpp
        test_instantiations.cpp
        unittest__union_find.cpp
        unittest__enode.cpp
        unittest__egraph.cpp
        unittest__extractor.cpp
        unittest__pattern.cpp
        unittest__match_arena.cpp
        unittest__vm_matching.cpp
        unittest__rewrite.cpp
        unittest__vm_compiler.cpp
        unittest__vm_trace.cpp
        unittest__vm_state.cpp
    LINK_TARGETS mg::planner ApprovalTests::ApprovalTests
)

# ApprovalTests needs the source directory to locate approved files.
target_compile_definitions(memgraph__unit__planner PRIVATE
    APPROVAL_TESTS_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
)
