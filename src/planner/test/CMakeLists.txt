# Header-only library exposing test fixtures and helpers.
# Consumed by unit tests here and by benchmark__egglog in bench/.
add_library(mg-planner-test-helpers INTERFACE)
add_library(mg::planner::test_helpers ALIAS mg-planner-test-helpers)
target_include_directories(mg-planner-test-helpers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mg-planner-test-helpers INTERFACE mg::planner GTest::gtest GTest::gmock)

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
    LINK_TARGETS mg::planner::test_helpers
)
