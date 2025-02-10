set(node2vec_online_utils
    ${CMAKE_CURRENT_SOURCE_DIR}/node2vec_online_utils/w2v_learners.py
    ${CMAKE_CURRENT_SOURCE_DIR}/node2vec_online_utils/walk_sampling.py
    ${CMAKE_CURRENT_SOURCE_DIR}/node2vec_online_utils/__init__.py
)

file(MAKE_DIRECTORY ${QUERY_MODULES_BUILD_DIR}/node2vec_online_utils)

add_custom_target(copy_node2vec_online_modules ALL
    COMMAND ${CMAKE_COMMAND} -E copy ${node2vec_online_utils} ${QUERY_MODULES_BUILD_DIR}/node2vec_online_utils
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/node2vec_online.py ${QUERY_MODULES_BUILD_DIR}
)

install(FILES ${node2vec_online_utils} DESTINATION ${QUERY_MODULES_DIR}/node2vec_online_utils)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/node2vec_online.py DESTINATION ${QUERY_MODULES_DIR})
