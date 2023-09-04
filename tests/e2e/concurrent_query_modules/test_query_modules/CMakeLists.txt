project(TestSharedObjects)
add_library(module_test SHARED module_test.cpp)
target_include_directories(module_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../../include)
