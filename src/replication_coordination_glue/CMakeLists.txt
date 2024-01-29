add_library(mg-repl_coord_glue STATIC )
add_library(mg::repl_coord_glue ALIAS mg-repl_coord_glue)

target_sources(mg-repl_coord_glue
        PUBLIC
        messages.hpp
        mode.hpp
        role.hpp

        PRIVATE
        messages.cpp
)

target_link_libraries(mg-repl_coord_glue mg-rpc mg-slk)
