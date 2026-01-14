################# Leiden community detection module

# Community detection module

set(leiden_community_detection_module_src
    leiden_community_detection_module.cpp
    algorithm/leiden.cpp
    leiden_utils/leiden_utils.cpp)

find_package(Boost CONFIG)

add_query_module(leiden_community_detection 1 "${leiden_community_detection_module_src}")
target_include_directories(leiden_community_detection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})
target_link_libraries(leiden_community_detection PRIVATE mg_utility)
