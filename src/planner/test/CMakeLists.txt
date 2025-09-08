add_unit_test(planner
    SOURCES
        unittest__union_find.cpp
        unittest__eclass.cpp
        unittest__enode.cpp
    LINK_TARGETS mg::planner
)
