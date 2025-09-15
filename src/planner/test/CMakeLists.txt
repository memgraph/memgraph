add_unit_test(planner
    SOURCES
        unittest__union_find.cpp
        unittest__eclass.cpp
        unittest__enode.cpp
        unittest__egraph.cpp
    LINK_TARGETS mg::planner
)

# Fuzzer for e-graph operations
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_executable(fuzz_egraph fuzz_egraph.cpp)
    target_link_libraries(fuzz_egraph PRIVATE mg::planner)
    target_compile_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_egraph PUBLIC ASSERT_FUZZ)

    # Optional: Create a corpus directory for the fuzzer
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus)
endif()
