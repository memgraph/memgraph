define_add_lcp(add_lcp_expr lcp_expr_cpp_files generated_lcp_expr_files)

add_lcp_expr(semantic/symbol.lcp)

add_custom_target(generate_lcp_expr DEPENDS ${generated_lcp_expr_files})

set(mg_expr_sources
    ${lcp_expr_cpp_files}
    parsing.cpp)

find_package(Boost REQUIRED)

add_library(mg-expr STATIC ${mg_expr_sources})
add_dependencies(mg-expr generate_lcp_expr)
target_include_directories(mg-expr PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mg-expr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mg-expr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/ast)
target_include_directories(mg-expr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/interpret)
target_include_directories(mg-expr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/semantic)
target_link_libraries(mg-expr cppitertools Boost::headers mg-utils mg-parser mg-functions)
