add_library(mg-memory-utils STATIC)
target_sources(mg-memory-utils
               PUBLIC
               query_memory_control.hpp
               global_memory_control.hpp
               PRIVATE
               query_memory_control.cpp
               global_memory_control.cpp)

add_library(mg-memory STATIC
            malloc_free.cpp
            new_delete.cpp)
message(STATUS "ENABLE_JEMALLOC: ${ENABLE_JEMALLOC}")
if (ENABLE_JEMALLOC)
  find_package(jemalloc REQUIRED)
  target_link_libraries(mg-memory-utils PRIVATE jemalloc::jemalloc ${CMAKE_DL_LIBS} mg-utils)
  target_compile_definitions(mg-memory-utils PUBLIC USE_JEMALLOC=1)
  target_link_libraries(mg-memory PRIVATE jemalloc::jemalloc ${CMAKE_DL_LIBS} mg-memory-utils fmt::fmt)
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=1)
else()
  target_link_libraries(mg-memory-utils PRIVATE mg-utils)
  target_compile_definitions(mg-memory-utils PUBLIC USE_JEMALLOC=0)
  target_link_libraries(mg-memory PRIVATE mg-memory-utils fmt::fmt)
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=0)
endif()
