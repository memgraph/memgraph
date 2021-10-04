set(memory_src_files
    new_delete.cpp
    memory_control.cpp)

add_library(mg-memory STATIC ${memory_src_files})
target_link_libraries(mg-memory mg-utils fmt)

if (ENABLE_JEMALLOC)
  target_link_libraries(mg-memory jemalloc)
endif()
