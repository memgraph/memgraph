set(cycles_src
    cycles_module.cpp
    algorithm/cycles.cpp)

add_query_module(cycles 1 "${cycles_src}")

# Link external libraries
target_link_libraries(cycles PRIVATE mg_utility)
target_include_directories(cycles PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
set(cycles_test_src
    cycles_test.cpp
    algorithm/cycles.cpp)

add_executable(cycles_test "${cycles_test_src}")
target_link_libraries(cycles_test PRIVATE mg_utility mage_gtest)
