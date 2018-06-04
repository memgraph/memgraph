# Kafka - openCypher clause

One must be able to specify the following when importing data from Kafka:
* Kafka URI
* Transform [script](transform.md) URI


Kafka endpoint is the URI of the leader broker and it is required for data
[extractor](extractor.md).

Minimum required syntax looks like:
```opencypher
CREATE STREAM kafka_stream AS LOAD DATA KAFKA '127.0.0.1/topic' WITH TRANSFORM
'127.0.0.1/transform.py';
```

The `CREATE STREAM` clause happens in a transaction.

The full openCypher clause for creating a stream is:
```opencypher
CREATE STREAM stream_name AS
  LOAD DATA KAFKA 'URI'
  WITH TRANSFORM 'URI'
  [BATCH INTERVAL milliseconds]
```

The `WITH TRANSFORM` parameter should contain a URI of the transform script.

The `BATCH_INTERVAL` parameter defines the time interval in milliseconds
that defines the time between two successive stream importing operations.

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

There are also convenience clauses to start and stop all streams:
```opencypher
START ALL STREAMS;
STOP ALL STREAMS;
```
