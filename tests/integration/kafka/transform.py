index_done = False

def stream(batch):
    global index_done
    ret = []
    if not index_done:
        ret.append(("CREATE INDEX ON :node(num)", {}))
        index_done = True
    for item in batch:
        message = item.decode("utf-8").strip().split()
        if len(message) == 1:
            ret.append(("MERGE (:node {num: $num})", {"num": message[0]}))
        elif len(message) == 2:
            ret.append(("MATCH (n:node {num: $num1}), (m:node {num: $num2}) MERGE (n)-[:et]->(m)", {"num1": message[0], "num2": message[1]}))
    return ret
