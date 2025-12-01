find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Boost REQUIRED CONFIG)

add_library(mg-communication-metrics STATIC metrics.cpp)
target_link_libraries(mg-communication-metrics nlohmann_json::nlohmann_json)

add_library(mg-communication STATIC)
target_sources(mg-communication
    PRIVATE
    websocket/auth.cpp
    websocket/server.cpp
    websocket/listener.cpp
    websocket/session.cpp
    bolt/v1/value.cpp
    bolt/v1/mg_types.cpp
    bolt/client.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp

    PUBLIC
    FILE_SET HEADERS
    FILES
    metrics.hpp
)
target_link_libraries(mg-communication Boost::headers Threads::Threads mg-utils mg-io mg-auth fmt::fmt gflags mg-communication-metrics mg-events mg-storage-v2)

target_link_libraries(mg-communication openssl::openssl)
