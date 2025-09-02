add_library(mg-planner)
add_library(mg::planner ALIAS mg-planner)

target_sources(mg-planner
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/planner/core/union_find.hpp

    PRIVATE
    src/core/union_find.cpp
)

if(BUILD_TESTING)
    add_subdirectory(test)
#    add_subdirectory(bench)
endif ()
