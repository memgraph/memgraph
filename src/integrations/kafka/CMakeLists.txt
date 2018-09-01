set(integrations_kafka_src_files
    consumer.cpp
    transform.cpp
    streams.cpp)

find_package(Seccomp)
if (NOT SECCOMP_FOUND)
    message(FATAL_ERROR "Couldn't find seccomp library!")
endif()

add_library(mg-integrations-kafka STATIC ${integrations_kafka_src_files})
target_link_libraries(mg-integrations-kafka stdc++fs Threads::Threads fmt
                      glog gflags librdkafka++ librdkafka zlib json)
target_link_libraries(mg-integrations-kafka mg-utils
                      mg-requests mg-communication)

target_link_libraries(mg-integrations-kafka ${Seccomp_LIBRARIES})
target_include_directories(mg-integrations-kafka SYSTEM PUBLIC ${Seccomp_INCLUDE_DIRS})

# Copy kafka.py to the root of our build directory where memgraph executable should be
configure_file(kafka.py ${CMAKE_BINARY_DIR} COPYONLY)
