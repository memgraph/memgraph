add_library(mg-arrow STATIC)
add_library(mg::arrow ALIAS mg-arrow)
target_sources(mg-arrow
        PUBLIC
        include/arrow_parquet/reader.hpp
        PRIVATE
        reader.cpp
)
target_include_directories(mg-arrow PUBLIC include)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

target_link_libraries(mg-arrow
        PUBLIC mg-utils
        PRIVATE Arrow::arrow_shared Parquet::parquet_shared
)
