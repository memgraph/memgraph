set(bridges_module_src
    bridges_module.cpp
    algorithm/bridges.cpp)

add_query_module(bridges 1 "${bridges_module_src}")

# Link external libraries
target_link_libraries(bridges PRIVATE mg_utility)
target_include_directories(bridges PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (NOT MAGE_CUGRAPH_ENABLE)
    include(GoogleTest)
    set(bridges_test_src
        bridges_test.cpp
        algorithm/bridges.cpp)

    add_executable(bridges_test "${bridges_test_src}")
    target_link_libraries(bridges_test PRIVATE mg_utility mage_gtest)
    gtest_add_tests(TARGET bridges_test)
endif()
