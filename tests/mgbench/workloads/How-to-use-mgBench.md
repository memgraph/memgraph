# How to use mgBench

Running your workloads that includes custom queries and dataset is the best way to evaluate system performance on your use-case. Each workload has special requirements that are imposed from the queries or dataset side. An basic difference would be a workload that needs to write a lot or it is just executing read queries from database.
We worked on cleaning MgBench architecture so it is easier for users to add theirs custom workloads and queries to evaluate performance on supported systems.

## How to add your custom workload

If you want to add you custom workload you start by writing a simple Python template class.

Here are 4 steps you need to do to specify your workload:

1. [Inherit the workload class](#1-inherit-the-workload-class)
2. [Define a workload name](#2-define-the-workload-name)
3. [Implement dataset generator method](#3-implement-dataset-generator-method)
4. [Define the queries you want to benchmark](#4-define-the-queries-you-want-to-benchmark)

Here is the basic [demo.py](https://github.com/memgraph/memgraph/blob/master/tests/mgbench/workloads/demo.py) example:

```python
import random
from workloads.base import Workload


class Demo(Workload):

    NAME = "demo"

    def dataset_generator(self):
        queries = [
                ("CREATE INDEX ON :NodeA(id);", {}),
                ("CREATE INDEX ON :NodeB(id);", {}),
            ]
        for i in range(0, 100):
            queries.append(("CREATE (:NodeA {id: $id});", {"id" : i}))
            queries.append(("CREATE (:NodeB {id: $id});", {"id" : i}))
        for i in range(0, 300):
            a = random.randint(0, 99)
            b = random.randint(0, 99)
            queries.append((("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id" : b}))

        return queries

    def benchmark__test__demo_query_get_nodes(self):
        return ("MATCH (n) RETURN n;", {})

    def benchmark__test__demo_query_get_node_by_id(self):
        return ("MATCH (n:NodeA{id: $id}) RETURN", {"id": random.randint(0, 99)})

```

The idea is to specify a simple Python class that contains your dataset generation queries and queries used for running a benchmark.

Let's break this script down into smaller important elements:

### 1. Inherit the `workload` class
The `Demo` script class has an parent class `Workload`. Each custom workload should inherit from the base `Workload` class.

```python
from workloads.base import Workload

class Demo(Workload):
```

### 2. Define the workload name
The class should specify the `NAME` property, this is used to describe what workload class you want to execute. When calling benchmark, this property will be important.

```python
NAME = "demo"
```

### 3. Implement dataset generator method
The class should implement the `dataset_generator()` method. The method is used for generating a dataset which returns the ***list of tuples***.  Each tuple contains string of the Cypher query and dictionary that contains optional arguments, so the structure is following [(str, dict), (str, dict)...]. Let's take a look at the how the example list could look like what could method return:

```python
queries = [
    ("CREATE INDEX ON (:NodeA(id));", {}),
    ("CREATE (:NodeA {id: 23});", {}),
    ("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" }),
    ...
]
```
As you can see you can pass just a Cypher query as pure string without any values in dictionary.

```python
("CREATE INDEX ON (:NodeA(id));", {}),
("CREATE (:NodeA {id: 23});", {}),
```

Or you can specified parameters inside a dictionary, the variables next to `$` sign in Query string will be replaced by the appropriate values behind the key from the dictionary. In this case `$id` is replaced by `123` and `$property` is replaced by `foo`. The dictionary key names and variable names need to match.

```python
("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" })
```


Back to our `demo.py` example, here is the `dataset_generator()` method, here you actually want to specify queries for generating a dataset. In this example the queries for index creation are generated first. In the first for loop the queries for creating 100 nodes with label `NodeA` and 100 nodes with label `NodeB` are prepared. Each node has `id` between 0 and 99. In the second for loop, queries for connecting nodes randomly are generated. There is total of 300 edges, each connected to random `NodeA` and `NodeB`.

```python
def dataset_generator(self):
    queries = [
            ("CREATE INDEX ON :NodeA(id);", {}),
            ("CREATE INDEX ON :NodeB(id);", {}),
        ]
    for i in range(0, 100):
        queries.append(("CREATE (:NodeA {id: $id});", {"id" : i}))
        queries.append(("CREATE (:NodeB {id: $id});", {"id" : i}))
    for i in range(0, 300):
        a = random.randint(0, 99)
        b = random.randint(0, 99)
        queries.append((("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id" : b}))

    return queries
```

### 4. Define the queries you want to benchmark

Now that your dataset will be imported from dataset generator queries, you can specify what queries you wish to benchmark on the given dataset. Here are two queries that `demo.py` workload specifies, they are written as Python methods that returns a single Tuple with query and dictionary.

```python
def benchmark__test__demo_query_get_nodes(self):
    return ("MATCH (n) RETURN n;", {})

def benchmark__test__demo_query_get_node_by_id(self):
    return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 99)})

```

The important details here are that each of the methods you wish to use for generating a workload

## How to run your custom workload
