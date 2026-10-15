# mg-value-order: the order a value carries, stated in terms of the value alone.
#
# Read by both the storage layer and the query layer, which hold a value
# differently and must still place a pair the same way. It takes no dependency
# of its own so that either can read it without taking one either.
add_library(mg-value-order INTERFACE)
add_library(mg::value-order ALIAS mg-value-order)

target_sources(mg-value-order
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS ../../
    FILES
    numbers.hpp
)
