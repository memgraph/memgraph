# Kafka - openCypher clause

One must be able to specify the following when importing data from Kafka:

* Kafka URI
* Kafka topic
* Transform [script](transform.md) URI


Minimum required syntax looks like:
```opencypher
CREATE STREAM stream_name AS LOAD DATA KAFKA 'URI'
  WITH TOPIC 'topic'
  WITH TRANSFORM 'URI';
```


The full openCypher clause for creating a stream is:
```opencypher
CREATE STREAM stream_name AS
  LOAD DATA KAFKA 'URI'
  WITH TOPIC 'topic'
  WITH TRANSFORM 'URI'
  [BATCH_INTERVAL milliseconds]
  [BATCH_SIZE count]
```
The `CREATE STREAM` clause happens in a transaction.

`WITH TOPIC` parameter specifies the Kafka topic from which we'll stream
data.

`WITH TRANSFORM` parameter should contain a URI of the transform script.

`BATCH_INTERVAL` parameter defines the time interval in milliseconds
which is the time between two successive stream importing operations.

`BATCH_SIZE` parameter defines the count of Kafka messages that will be
batched together before import.

If both `BATCH_INTERVAL` and `BATCH_SIZE` parameters are given, the condition
that is satisfied first will trigger the batched import.

Default value for `BATCH_INTERVAL` is 100 milliseconds, and the default value
for `BATCH_SIZE` is 10;

The `DROP` clause deletes a stream:
```opencypher
DROP STREAM stream_name;
```

The `SHOW` clause enables you to see all configured streams:
```opencypher
SHOW STREAMS;
```

You can also start/stop streams with the `START` and `STOP` clauses:
```opencypher
START STREAM stream_name [LIMIT count BATCHES];
STOP STREAM stream_name;
```
A stream needs to be stopped in order to start it and it needs to be started in
order to stop it. Starting a started or stopping a stopped stream will not
affect that stream.

There are also convenience clauses to start and stop all streams:
```opencypher
START ALL STREAMS;
STOP ALL STREAMS;
```

Before the actual import, you can also test the stream with the `TEST
STREAM` clause:
```opencypher
TEST STREAM stream_name [LIMIT count BATCHES];
```
When a stream is tested, data extraction and transformation occurs, but no
output is inserted in the graph.

A stream needs to be stopped in order to test it. When the batch limit is
omitted, `TEST STREAM` will run for only one batch by default.
