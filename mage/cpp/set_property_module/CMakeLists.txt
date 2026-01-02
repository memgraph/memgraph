include(GNUInstallDirs)

set(set_property_module_src
    set_property_module.cpp)

add_query_module(set_property 1 "${set_property_module_src}")

target_link_libraries(set_property PRIVATE fmt::fmt)
