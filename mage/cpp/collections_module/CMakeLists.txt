set(collections_src
    collections_module.cpp
    algorithm/collections.cpp)

add_query_module(collections 1 "${collections_src}")

# fmt for type-name formatting in error messages
target_link_libraries(collections PRIVATE fmt::fmt)

# Make fmt headers system headers for this target
target_include_directories(collections SYSTEM PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>
)
