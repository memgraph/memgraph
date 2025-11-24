add_library(mg-csv STATIC)
add_library(mg::csv ALIAS mg-csv)
target_sources(mg-csv
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS include
        FILES include/csv/parsing.hpp

        PRIVATE
        parsing.cpp
        )
target_include_directories(mg-csv PUBLIC include)

find_package(Boost REQUIRED CONFIG COMPONENTS iostreams)
find_package(ctre)
target_link_libraries(mg-csv
        PUBLIC mg-utils
        PRIVATE AWS::aws-sdk-cpp-core AWS::aws-sdk-cpp-s3 ctre::ctre mg-requests Boost::iostreams
        )

add_subdirectory(fuzz EXCLUDE_FROM_ALL)
