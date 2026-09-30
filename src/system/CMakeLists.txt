add_library(mg-system STATIC)
add_library(mg::system ALIAS mg-system)
target_sources(mg-system
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS include
        FILES
        include/system/action.hpp
        include/system/system.hpp
        include/system/transaction.hpp
        include/system/state.hpp
        include/system/rpc.hpp

        PRIVATE
        action.cpp
        system.cpp
        transaction.cpp
        state.cpp
        rpc.cpp
)

target_link_libraries(mg-system
        PUBLIC
        mg::replication
)
