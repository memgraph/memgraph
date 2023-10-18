find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

set(communication_src_files
    websocket/auth.cpp
    websocket/server.cpp
    websocket/listener.cpp
    websocket/session.cpp
    bolt/v1/value.cpp
    bolt/client.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp)

find_package(Boost REQUIRED)

add_library(mg-communication-metrics STATIC metrics.cpp)
target_link_libraries(mg-communication-metrics json)

add_library(mg-communication STATIC ${communication_src_files})
target_link_libraries(mg-communication Boost::headers Threads::Threads mg-utils mg-io mg-auth fmt::fmt gflags mg-communication-metrics mg-events)

find_package(OpenSSL REQUIRED)
target_link_libraries(mg-communication ${OPENSSL_LIBRARIES})
target_include_directories(mg-communication SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
target_precompile_headers(mg-communication INTERFACE http/server.hpp <boost/beast/websocket.hpp> bolt/v1/session.hpp)
