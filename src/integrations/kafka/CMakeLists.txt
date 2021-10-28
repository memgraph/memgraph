set(integrations_kafka_src_files
    consumer.cpp
)

add_library(mg-integrations-kafka STATIC ${integrations_kafka_src_files})
target_link_libraries(mg-integrations-kafka mg-utils librdkafka++ librdkafka Threads::Threads)
