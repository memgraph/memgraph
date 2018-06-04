# Kafka - data extractor

The data extractor is responsible for loading data from Kafka.  In order to do
so, it needs to know the URI of the Kafka leader broker.  Once the extractor
connects to Kafka, it starts importing data.

Data extractor depends on [cppkafka](https://github.com/mfontanini/cppkafka)
which makes message consumption just a few API calls, as seen
[here](https://github.com/mfontanini/cppkafka/wiki/Consuming-messages).

There are also other metadata that can be passed to data extractor that are
defined with our [extension](opencypher.md) of openCypher.

A full list of configurable metadata can be found
[here](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

Memgraph supports customizing the following:

  * `metadata.broker.list` which is a required parameter, set by `KAFKA 'URI'`
  * `queue.buffering.max.ms` set by `BATCH INTERVAL`
