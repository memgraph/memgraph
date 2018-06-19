set(integrations_src_files
    kafka/streams.cpp
    kafka/consumer.cpp)

add_library(mg-integrations STATIC ${integrations_src_files})
target_link_libraries(mg-integrations Threads::Threads fmt glog gflags
                      librdkafka++ librdkafka zlib)
