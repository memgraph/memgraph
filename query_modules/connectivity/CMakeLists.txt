set(MODULE src/connectivity_module.cpp)

include_directories(src)

add_library(connectivity SHARED ${MODULE})
target_include_directories(connectivity PRIVATE ${CMAKE_SOURCE_DIR}/include)

install(PROGRAMS $<TARGET_FILE:connectivity>
        DESTINATION lib/memgraph/query_modules
        RENAME connectivity.so)
