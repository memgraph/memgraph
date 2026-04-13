add_library(mg-repl_coord_glue STATIC )
add_library(mg::repl_coord_glue ALIAS mg-repl_coord_glue)

target_sources(mg-repl_coord_glue
        PUBLIC
        messages.hpp
        mode.hpp
        role.hpp
        handler.hpp
        common.hpp

        PRIVATE
        messages.cpp
        mode.cpp
        role.cpp
)

find_package(range-v3 REQUIRED)
target_link_libraries(mg-repl_coord_glue mg-rpc mg-slk range-v3::range-v3)
