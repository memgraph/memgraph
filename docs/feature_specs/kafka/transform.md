# Kafka - data transform

The transform script is a user defined script written in Python.  The script
should be aware of the data format in the Kafka message.

Each Kafka message is byte length encoded, which means that the first eight
bytes of each message contain the length of the message.

More on the message format can be seen
[here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets).


The script will be embedded in our C++ codebase using pythons
[embedding](https://docs.python.org/3.5/extending/embedding.html) feature.

A sample code for a streaming transform script could look like this:

```python
import struct
import sys

def get_records():
    while True:
        message_len = sys.stdin.read(8)
        if len(message_len) == 8:
            message_len = struct.unpack("L", message_len)[0]
            record = sys.stdin.read(message_len)
            yield record
        else:
            assert len(message_len) == 0, message_len
            return

def create_vertex(fields):
  return "CREATE (n:Node {{id: {}}})".format(fields[1])


def create_edge(fields):
  return "MATCH (n:Node {{id: {}}}) "\
         "MATCH ((m:Node {{id : {}}})) "\
         "CREATE (n)-[e:Edge{{value: {}}}]->(m) "\
         .format(fields[1], fields[2], fields[3])

for record in get_records():
  fields = record.split("\t")
  if fields[0] == "v":
    return create_vertex(fields):
  else:
    return create_edge(fields)
```

The script should output openCypher query strings based on the type of the
records.
