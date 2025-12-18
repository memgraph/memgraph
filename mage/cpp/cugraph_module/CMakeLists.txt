add_query_module(cugraph.pagerank 1 algorithms/pagerank.cu)
target_mage_cugraph(cugraph.pagerank)
target_link_libraries(cugraph.pagerank PRIVATE mg_utility)
target_include_directories(cugraph.pagerank PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.personalized_pagerank 1 algorithms/personalized_pagerank.cu)
target_mage_cugraph(cugraph.personalized_pagerank)
target_link_libraries(cugraph.personalized_pagerank PRIVATE mg_utility)
target_include_directories(cugraph.personalized_pagerank PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.louvain 1 algorithms/louvain.cu)
target_mage_cugraph(cugraph.louvain)
target_link_libraries(cugraph.louvain PRIVATE mg_utility)
target_include_directories(cugraph.louvain PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.katz_centrality 1 algorithms/katz_centrality.cu)
target_mage_cugraph(cugraph.katz_centrality)
target_link_libraries(cugraph.katz_centrality PRIVATE mg_utility)
target_include_directories(cugraph.katz_centrality PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.leiden 1 algorithms/leiden.cu)
target_mage_cugraph(cugraph.leiden)
target_link_libraries(cugraph.leiden PRIVATE mg_utility)
target_include_directories(cugraph.leiden PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.betweenness_centrality 1 algorithms/betweenness_centrality.cu)
target_mage_cugraph(cugraph.betweenness_centrality)
target_link_libraries(cugraph.betweenness_centrality PRIVATE mg_utility)
target_include_directories(cugraph.betweenness_centrality PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.balanced_cut_clustering 1 algorithms/balanced_cut_clustering.cu)
target_mage_cugraph(cugraph.balanced_cut_clustering)
target_link_libraries(cugraph.balanced_cut_clustering PRIVATE mg_utility)
target_include_directories(cugraph.balanced_cut_clustering PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.spectral_clustering 1 algorithms/spectral_clustering.cu)
target_mage_cugraph(cugraph.spectral_clustering)
target_link_libraries(cugraph.spectral_clustering PRIVATE mg_utility)
target_include_directories(cugraph.spectral_clustering PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.hits 1 algorithms/hits.cu)
target_mage_cugraph(cugraph.hits)
target_link_libraries(cugraph.hits PRIVATE mg_utility)
target_include_directories(cugraph.hits PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_query_module(cugraph.generator 1 algorithms/graph_generator.cu)
target_mage_cugraph(cugraph.generator)
target_link_libraries(cugraph.generator PRIVATE mg_utility)
target_include_directories(cugraph.generator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
