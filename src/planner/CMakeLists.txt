add_library(mg-planner)
add_library(mg::planner ALIAS mg-planner)

target_sources(mg-planner
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS include
    FILES
    include/planner/core/union_find.hpp
    include/planner/core/enode.hpp

    PRIVATE
    src/core/union_find.cpp
    src/core/enode.cpp
)

target_link_libraries(mg-planner PRIVATE mg-utils)

if(MG_ENABLE_TESTING)
  add_subdirectory(test)
  add_subdirectory(bench)
endif()
