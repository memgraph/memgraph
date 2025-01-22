################# LabelRankT

# Community detection online module
set(community_detection_online_module_src
    community_detection_online_module.cpp
    algorithm_online/community_detection.cpp)

add_query_module(community_detection_online 1 "${community_detection_online_module_src}")

target_include_directories(community_detection_online PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
