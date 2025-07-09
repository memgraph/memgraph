set(integrations_kafka_src_files
    consumer.cpp
)
find_package(RdKafka REQUIRED)

add_library(mg-integrations-kafka STATIC ${integrations_kafka_src_files})
target_link_libraries(mg-integrations-kafka mg-utils RdKafka::rdkafka++ librdkafka Threads::Threads)
