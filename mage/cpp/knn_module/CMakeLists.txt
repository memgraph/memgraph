set(knn_module_src
    knn_module.cpp
    algorithms/knn.hpp)

add_query_module(knn 1 "${knn_module_src}")

# Find OpenMP
find_package(OpenMP REQUIRED)

# Link external libraries
target_link_libraries(knn PRIVATE mg_utility fmt::fmt OpenMP::OpenMP_CXX)

# Make fmt headers system headers for this target
target_include_directories(knn SYSTEM PRIVATE
  $<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>
)

target_include_directories(knn PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
