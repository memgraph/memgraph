# Kafka - data transform

The transform script is a user defined script written in Python.  The script
should be aware of the data format in the Kafka message.

Each Kafka message is byte length encoded, which means that the first eight
bytes of each message contain the length of the message.

A sample code for a streaming transform script could look like this:

```python
def create_vertex(vertex_id):
  return ("CREATE (:Node {id: $id})", {"id": vertex_id})


def create_edge(from_id, to_id):
  return ("MATCH (n:Node {id: $from_id}), (m:Node {id: $to_id}) "\
          "CREATE (n)-[:Edge]->(m)", {"from_id": from_id, "to_id": to_id})


def stream(batch):
    result = []
    for item in batch:
        message = item.decode('utf-8').strip().split()
        if len(message) == 1:
          result.append(create_vertex(message[0])))
        else:
          result.append(create_edge(message[0], message[1]))
    return result

```

The script should output openCypher query strings based on the type of the
records.
