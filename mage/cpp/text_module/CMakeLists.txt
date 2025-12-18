set(text_module_src
    text_module.cpp
    algorithm/text.cpp)

add_query_module(text 1 "${text_module_src}")
target_link_libraries(text PRIVATE fmt::fmt)
target_include_directories(text PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
