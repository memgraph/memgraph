add_library(mg-wire-format STATIC
    framed_codec.cpp
)
add_library(mg::wire_format ALIAS mg-wire-format)

target_link_libraries(mg-wire-format PUBLIC mg-slk)
