set(mg_glue_sources auth.cpp auth_checker.cpp auth_handler.cpp communication.cpp)

add_library(mg-glue STATIC ${mg_glue_sources})
target_link_libraries(mg-glue mg-query mg-auth)
