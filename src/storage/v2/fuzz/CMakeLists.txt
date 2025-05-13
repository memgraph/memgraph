add_executable(fuzz_property_store EXCLUDE_FROM_ALL)
target_sources(fuzz_property_store
    PRIVATE fuzz_property_store.cpp

    PRIVATE FILE_SET HEADERS
    FILES
    helpers/read_parse.hpp
)
target_include_directories(fuzz_property_store PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(fuzz_property_store PRIVATE mg::storage)
target_compile_options(fuzz_property_store PRIVATE -fsanitize=fuzzer)
target_link_libraries(fuzz_property_store PRIVATE -fsanitize=fuzzer)
