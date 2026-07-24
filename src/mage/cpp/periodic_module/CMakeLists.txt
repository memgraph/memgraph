include(GNUInstallDirs)

# Add all module files related to graph util module
set(periodic_src
    periodic.cpp)

mage_add_query_module(periodic 1 "${periodic_src}")

target_link_libraries(periodic PRIVATE fmt::fmt)
