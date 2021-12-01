set(communication_src_files
    bolt/v1/value.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp)

add_library(mg-communication STATIC ${communication_src_files})
target_link_libraries(mg-communication Threads::Threads mg-utils mg-io fmt::fmt gflags)

find_package(OpenSSL REQUIRED)
target_link_libraries(mg-communication ${OPENSSL_LIBRARIES})
target_include_directories(mg-communication SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
