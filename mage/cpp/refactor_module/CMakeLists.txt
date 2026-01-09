set(refactor_module_src
    refactor_module.cpp
    algorithm/refactor.cpp)

add_query_module(refactor 1 "${refactor_module_src}")

target_link_libraries(refactor PRIVATE fmt::fmt)

# Make fmt headers system headers for this target
target_include_directories(refactor SYSTEM PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>
)
