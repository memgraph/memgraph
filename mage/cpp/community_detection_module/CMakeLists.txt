################# Louvain

# Community detection module

set(community_detection_module_src
    community_detection_module.cpp
    algorithm/louvain.cpp)

add_subdirectory(grappolo)

add_query_module(community_detection 1 "${community_detection_module_src}")
target_include_directories(community_detection PRIVATE ${GRAPPOLO_HEADERS} ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(community_detection PRIVATE mg_utility basic_cd full_syn_opt coloring util OpenMP::OpenMP_CXX)
