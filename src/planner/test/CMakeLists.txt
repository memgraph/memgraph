add_unit_test(planner
    SOURCES
        unittest__union_find.cpp
        unittest__enode.cpp
        unittest__egraph.cpp
        unittest__extractor.cpp
        unittest__pattern.cpp
        unittest__match_arena.cpp
        unittest__ematch.cpp
        unittest__rewrite.cpp
        unittest__join_order.cpp
        unittest__vm_compiler.cpp
        unittest__vm_executor.cpp
        unittest__vm_state.cpp
        unittest__vm_comparison.cpp
    LINK_TARGETS mg::planner
)
