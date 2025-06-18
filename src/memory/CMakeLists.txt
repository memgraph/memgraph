set(memory_src_files
    malloc_free.cpp
    new_delete.cpp
    global_memory_control.cpp
    query_memory_control.cpp)

add_library(mg-memory STATIC ${memory_src_files})

message(STATUS "ENABLE_JEMALLOC: ${ENABLE_JEMALLOC}")
if (ENABLE_JEMALLOC)
  find_package(jemalloc REQUIRED)
  target_link_libraries(mg-memory PRIVATE Jemalloc::Jemalloc ${CMAKE_DL_LIBS})
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=1)
else()
  target_compile_definitions(mg-memory PUBLIC USE_JEMALLOC=0)
endif()
