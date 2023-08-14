add_library(mg-glue STATIC )
target_sources(mg-glue PRIVATE auth.cpp auth_checker.cpp auth_handler.cpp communication.cpp SessionHL.cpp ServerT.cpp)
target_link_libraries(mg-glue mg-query mg-auth)
