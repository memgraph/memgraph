add_library(mg-planner-test-support OBJECT)
add_library(mg::planner::test_support ALIAS mg-planner-test-support)

target_sources(mg-planner-test-support
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/..
    FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/all.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/builders.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/extract.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/patterns.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/symbols.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/types.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/op_make_traits.hpp

    PRIVATE
    instantiations.cpp
)

target_link_libraries(mg-planner-test-support PUBLIC mg::planner)
