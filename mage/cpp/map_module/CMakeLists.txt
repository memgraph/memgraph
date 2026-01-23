set(map_module_src
    map_module.cpp
    algorithm/map.cpp)

add_query_module(map 1 "${map_module_src}")

target_link_libraries(map PRIVATE fmt::fmt)

# Make fmt headers system headers for this target
target_include_directories(map SYSTEM PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>
)
