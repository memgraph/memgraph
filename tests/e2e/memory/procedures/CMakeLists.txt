add_library(global_memory_limit SHARED global_memory_limit.c)
target_include_directories(global_memory_limit PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_library(global_memory_limit_proc SHARED global_memory_limit_proc.c)
target_include_directories(global_memory_limit_proc PRIVATE ${CMAKE_SOURCE_DIR}/include)
