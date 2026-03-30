add_library(mg-memory-utils STATIC)
target_sources(mg-memory-utils
               PUBLIC
               query_memory_control.hpp
               global_memory_control.hpp
               db_arena.hpp
               PRIVATE
               query_memory_control.cpp
               global_memory_control.cpp
               db_arena.cpp)

add_library(mg-memory STATIC
            malloc_free.cpp
            new_delete.cpp)
message(STATUS "ENABLE_JEMALLOC: ${ENABLE_JEMALLOC}")
target_link_libraries(mg-memory-utils PUBLIC mg-jemalloc-defs)
target_link_libraries(mg-memory PUBLIC mg-jemalloc-defs)
if (ENABLE_JEMALLOC)
  find_package(jemalloc REQUIRED)
  target_link_libraries(mg-memory-utils PRIVATE Jemalloc::Jemalloc ${CMAKE_DL_LIBS} mg-utils)
  target_link_libraries(mg-memory PRIVATE Jemalloc::Jemalloc ${CMAKE_DL_LIBS} mg-memory-utils fmt::fmt)
else()
  target_link_libraries(mg-memory-utils PRIVATE mg-utils)
  target_link_libraries(mg-memory PRIVATE mg-memory-utils fmt::fmt)
endif()
