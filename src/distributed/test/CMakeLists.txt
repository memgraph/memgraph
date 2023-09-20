add_executable(distributed_tests)
target_link_libraries(distributed_tests PRIVATE mg::distributed)
target_sources(distributed_tests PRIVATE test.cpp)
add_test(NAME distributed_unittest COMMAND $<TARGET_FILE:distributed_tests>)
