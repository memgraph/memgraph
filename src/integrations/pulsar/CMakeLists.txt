set(integrations_pulsar_src_files
    consumer.cpp
)

find_package(CURL REQUIRED)

add_library(mg-integrations-pulsar STATIC ${integrations_pulsar_src_files})
target_link_libraries(mg-integrations-pulsar mg-utils pulsar Snappy::snappy protobuf Threads::Threads ${CURL_LIBRARIES})
