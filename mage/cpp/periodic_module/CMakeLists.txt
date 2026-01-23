include(GNUInstallDirs)

# Add all module files related to graph util module
set(periodic_src
    periodic.cpp)

add_query_module(periodic 1 "${periodic_src}")

target_link_libraries(periodic PRIVATE fmt::fmt)

# Make fmt headers system headers for this target
target_include_directories(periodic SYSTEM PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>
)
