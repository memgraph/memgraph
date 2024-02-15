add_library(mg-system STATIC)
add_library(mg::system ALIAS mg-system)
target_sources(mg-system
        PUBLIC
        include/system/action.hpp
        include/system/system.hpp
        include/system/transaction.hpp
        include/system/state.hpp

        PRIVATE
        action.cpp
        system.cpp
        transaction.cpp
        state.cpp

)
target_include_directories(mg-system PUBLIC include)

target_link_libraries(mg-system
        PUBLIC
        mg::replication

)
