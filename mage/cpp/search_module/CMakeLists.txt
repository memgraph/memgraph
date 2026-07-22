set(search_module_src
    search_module.cpp
    algorithm/search.cpp)

add_query_module(search 1 "${search_module_src}")

target_include_directories(search PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
