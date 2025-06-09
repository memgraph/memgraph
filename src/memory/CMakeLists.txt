message(STATUS "ENABLE_JEMALLOC: ${ENABLE_JEMALLOC}")

add_library(mg-malloc-shared SHARED malloc_free.cpp)
add_library(mg::malloc::shared ALIAS mg-malloc-shared)
find_package(jemalloc REQUIRED)
target_link_libraries(mg-malloc-shared Jemalloc::Jemalloc ${CMAKE_DL_LIBS})

add_library(mg-malloc STATIC malloc_free.cpp)
add_library(mg::malloc ALIAS mg-malloc)
find_package(jemalloc REQUIRED)
target_link_libraries(mg-malloc Jemalloc::Jemalloc ${CMAKE_DL_LIBS})

set(memory_src_files
    new_delete.cpp
    global_memory_control.cpp
    query_memory_control.cpp)

add_library(mg-memory STATIC ${memory_src_files})
target_link_libraries(mg-memory mg-utils fmt mg-malloc)

message(STATUS "ENABLE_JEMALLOC: ${ENABLE_JEMALLOC}")
if (ENABLE_JEMALLOC)
  # find_package(jemalloc REQUIRED)
  target_link_libraries(mg-memory Jemalloc::Jemalloc ${CMAKE_DL_LIBS})
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=1)
else()
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=0)
endif()
