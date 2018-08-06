set(integrations_src_files
    kafka/streams.cpp
    kafka/transform.cpp
    kafka/consumer.cpp)

include(FindSeccomp.cmake)
if (NOT SECCOMP_FOUND)
    message(FATAL_ERROR "Couldn't find seccomp library!")
endif()

add_library(mg-integrations STATIC ${integrations_src_files})
target_link_libraries(mg-integrations stdc++fs Threads::Threads fmt
                      glog gflags librdkafka++ librdkafka zlib json)
target_link_libraries(mg-integrations mg-utils mg-requests mg-communication)

target_link_libraries(mg-integrations ${Seccomp_LIBRARIES})
target_include_directories(mg-integrations SYSTEM PUBLIC ${Seccomp_INCLUDE_DIRS})

# Copy kafka.py to the root of our build directory where memgraph executable should be
configure_file(kafka/kafka.py ${CMAKE_BINARY_DIR} COPYONLY)
