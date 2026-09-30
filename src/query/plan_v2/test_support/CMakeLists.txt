add_library(mg-query-plan-v2-test-support INTERFACE)
add_library(mg::query::plan_v2::test_support ALIAS mg-query-plan-v2-test-support)

target_include_directories(mg-query-plan-v2-test-support
    INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)

target_link_libraries(mg-query-plan-v2-test-support INTERFACE mg-query mg::planner)
