## Integrations

### Kafka

Apache Kafka is an open-source stream-processing software platform. The project
aims to provide a unified, high-throughput, low-latency platform for handling
real-time data feeds.

Memgraph offers easy data import at the source using Kafka as the
high-throughput messaging system.

#### openCypher

Memgraphs custom openCypher clause for creating a stream is:
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
We cover more about the transform script later, in the [transform](#transform)
section.

`BATCH_INTERVAL` parameter defines the time interval in milliseconds
which is the time between two successive stream importing operations.

`BATCH_SIZE` parameter defines the count of Kafka messages that will be
batched together before import.

If both `BATCH_INTERVAL` and `BATCH_SIZE` parameters are given, the condition
that is satisfied first will trigger the batched import.

Default value for `BATCH_INTERVAL` is 100 milliseconds, and the default value
for `BATCH_SIZE` is 10.

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
When a stream is tested, data extraction and transformation occurs, but nothing
is inserted into the graph.

A stream needs to be stopped in order to test it. When the batch limit is
omitted, `TEST STREAM` will run for only one batch by default.

#### Transform

The transform script allows Memgraph users to have custom Kafka messages and
still be able to import data in Memgraph by adding the logic to decode the
messages in the transform script.

The entry point of the transform script from Memgraph is the `stream` function.
Input for the `stream` function is a list of bytes that represent byte encoded
Kafka messages, and the output of the `stream` function must be a list of
tuples containing openCypher string queries and corresponding parameters stored
in a dictionary.

To be more precise, the signature of the `stream` function looks like the
following:
```plaintext
stream : [bytes] -> [(str, {str : type})]
type : none | bool | int | float | str | list | dict
```

An example of a simple transform script that creates vertices if the message
contains one number (the vertex id) or it creates edges if the message contains
two numbers (origin vertex id and destination vertex id) would look like the
following:
```python
def create_vertex(vertex_id):
  return ("CREATE (:Node {id: $id})", {"id": vertex_id})


def create_edge(from_id, to_id):
  return ("MATCH (n:Node {id: $from_id}), (m:Node {id: $to_id}) "\
          "CREATE (n)-[:Edge]->(m)", {"from_id": from_id, "to_id": to_id})


def stream(batch):
    result = []
    for item in batch:
        message = item.decode('utf-8').split()
        if len(message) == 1:
          result.append(create_vertex(message[0]))
        elif len(message) == 2:
          result.append(create_edge(message[0], message[1]))
    return result
```

#### Example

For this example, we assume you have a local instance of Kafka. You can find
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
Similar, if the given Kafka topic doesn't exist, we'll get the following:
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
