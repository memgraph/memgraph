set(bitwise_module_src
    bitwise_module.cpp
    algorithm/bitwise.cpp)

mage_add_query_module(bitwise 1 "${bitwise_module_src}")

target_include_directories(bitwise PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Module tests
if (MG_MAGE_BUILD_TESTS)
    include(GoogleTest)
    set(bitwise_test_src
        bitwise_test.cpp
        algorithm/bitwise.cpp)

    add_executable(bitwise_test "${bitwise_test_src}")
    target_link_libraries(bitwise_test PRIVATE mage_gtest)
    gtest_add_tests(TARGET bitwise_test TEST_PREFIX mage__)
endif()
