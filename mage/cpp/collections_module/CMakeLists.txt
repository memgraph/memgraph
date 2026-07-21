set(collections_src
    collections_module.cpp
    algorithm/collections.cpp)

add_query_module(collections 1 "${collections_src}")

# fmt for type-name formatting in error messages
target_link_libraries(collections PRIVATE fmt::fmt)
