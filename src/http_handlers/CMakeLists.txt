set(mg_http_handlers_sources)

add_library(mg-http-handlers STATIC ${mg_http_handlers_sources})
target_link_libraries(mg-http-handlers mg-query mg-storage-v2 mg-events)
