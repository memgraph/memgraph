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
if (ENABLE_JEMALLOC)
  target_link_libraries(mg-memory-utils PUBLIC jemalloc::jemalloc PRIVATE ${CMAKE_DL_LIBS} mg-utils)
  target_compile_definitions(mg-memory-utils PUBLIC USE_JEMALLOC=1)
  target_link_libraries(mg-memory PRIVATE jemalloc::jemalloc ${CMAKE_DL_LIBS} mg-memory-utils fmt::fmt)
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=1)
  # malloc is interposed here and reads thread-locals its caller sets. The compiler's model of a
  # standard allocation function says it cannot touch caller-visible memory, which would let it
  # drop those stores, so the allocation functions this file defines are not treated as builtins.
  target_compile_options(mg-memory PRIVATE
                         -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc
                         -fno-builtin-free -fno-builtin-aligned_alloc)
else()
  target_link_libraries(mg-memory-utils PRIVATE mg-utils)
  target_compile_definitions(mg-memory-utils PUBLIC USE_JEMALLOC=0)
  target_link_libraries(mg-memory PRIVATE mg-memory-utils fmt::fmt)
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=0)
endif()
