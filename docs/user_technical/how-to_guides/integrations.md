## Integrations

### Kafka

Apache Kafka is an open-source stream-processing software platform. The project
aims to provide a unified, high-throughput, low-latency platform for handling
real-time data feeds.

Memgraph offers easy data import at the source using Kafka as the
high-throughput messaging system.

At this point, we strongly advise you to read the streaming section of our
[reference guide](../reference_guide/streaming.md)

In this article, we assume you have a local instance of Kafka. You can find
more about running Kafka [here](https://kafka.apache.org/quickstart).

From this point forth, we assume you have a instance of Kafka running on
`localhost:9092` with a topic `test` and that you've started Memgraph and have
Memgraph client running.

Each Kafka stream in Memgraph requires a transform script written in `Python`
that knows how to interpret incoming data and transform the data to queries that
Memgraph understands. Lets assume you have script available on
`http://localhost/transform.py`.

Lets also assume the Kafka topic contains two types of messages:

  * Node creation: the message contains a single number, the node id.
  * Edge creation: the message contains two numbers, origin node id and
    destination node id.

In order to create a stream input the following query in the client:

```opencypher
CREATE STREAM mystream AS LOAD DATA KAFKA 'localhost:9092' WITH TOPIC 'test' WITH
TRANSFORM 'http://localhost/transform.py'
```

This will create the stream inside Memgraph but will not start it yet. However,
if the Kafka instance isn't available on the given URI, or the topic doesn't
exist, the query will fail with an appropriate message.

E.g. if the transform script can't be found at the given URI, the following
error will be shown:

```plaintext
Client received exception: Couldn't get the transform script from http://localhost/transform.py
```
Similarly, if the given Kafka topic doesn't exist, we'll get the following:

```plaintext
Client received exception: Kafka stream mystream, topic not found
```

After a successful stream creation, you can check the status of all streams by
executing:

```opencypher
SHOW STREAMS
```

This should produce the following output:

```plaintext
+----------+----------------+-------+------------------------------+---------+
| name     | uri            | topic | transform                    | status  |
+---------------------------+--------------------------------------+---------+
| mystream | localhost:9092 | test  | http://localhost/memgraph.py | stopped |
+----------+----------------+-------+------------------------------+---------+
```
As you can notice, the status of this stream is stopped.

In order to see if everything is correct, you can test the stream by executing:

```opencypher
TEST STREAM mystream;
```

This will ingest data from Kafka, but instead of writing it to Memgraph, it will
just output the result.

If the `test` Kafka topic would contain two messages, `1` and `1 2` the result
of the `TEST STREAM` query would look like:

```plaintext
+-------------------------------------------------------------------------------+-------------------------+
| query                                                                         | params                  |
+-------------------------------------------------------------------------------+-------------------------+
| CREATE (:Node {id: $id})                                                      | {id:"1"}                |
| MATCH (n:Node {id: $from_id}), (m:Node {id: $to_id}) CREATE (n)-[:Edge]->(m)  | {from_id:"1",to_id:"2"} |
+-------------------------------------------------------------------------------+-------------------------+
```

To start ingesting data from a stream, you need to execute the following query:

```opencypher
START STREAM mystream;
```

If we check the stream status now, the output would look like this:

```plaintext
+----------+----------------+-------+------------------------------+---------+
| name     | uri            | topic | transform                    | status  |
+---------------------------+--------------------------------------+---------+
| mystream | localhost:9092 | test  | http://localhost/memgraph.py | running |
+----------+----------------+-------+------------------------------+---------+
```

To stop ingesting data, the stop stream query needs to be executed:

```opencypher
STOP STREAM mystream;
```

If Memgraph shuts down, all streams that existed before the shutdown are going
to be recovered.
