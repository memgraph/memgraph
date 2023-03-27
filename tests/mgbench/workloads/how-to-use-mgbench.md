# How to use mgBench

Running your workloads that includes custom queries and dataset is the best way to evaluate system performance on your use-case. Each workload has special requirements that are imposed from the queries or dataset side. An basic difference would be a workload that needs to write a lot or it is just executing read queries from database.
We worked on cleaning MgBench architecture so it is easier for users to add theirs custom workloads and queries to evaluate performance on supported systems.

## How to add your custom workload

If you want to run your custom workload on Memgraph na Neo4j, you can start by writing a simple Python class.

Here are 4 steps you need to do to specify your workload:

1. [Inherit the workload class](#1-inherit-the-workload-class)
2. [Define a workload name](#2-define-the-workload-name)
3. [Implement dataset generator method](#3-implement-dataset-generator-method)
4. [Implement index generator method](#4-implement-the-index-generator)
5. [Define the queries you want to benchmark](#4-define-the-queries-you-want-to-benchmark)

Here is th simplified version of [demo.py](https://github.com/memgraph/memgraph/blob/master/tests/mgbench/workloads/demo.py) example:

```python
import random
from workloads.base import Workload


class Demo(Workload):

    NAME = "demo"

    def indexes_generator(self):
        indexes = [
                    ("CREATE INDEX ON :NodeA(id);", {}),
                    ("CREATE INDEX ON :NodeB(id);", {}),
                ]
        return indexes

    def dataset_generator(self):

        queries = []
        for i in range(0, 100):
            queries.append(("CREATE (:NodeA {id: $id});", {"id": i}))
            queries.append(("CREATE (:NodeB {id: $id});", {"id": i}))
        for i in range(0, 300):
            a = random.randint(0, 99)
            b = random.randint(0, 99)
            queries.append(
                (("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id": b})
            )

        return queries

    def benchmark__test__get_nodes(self):
        return ("MATCH (n) RETURN n;", {})

    def benchmark__test__get_node_by_id(self):
        return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 99)})


```

The idea is to specify a simple Python class that contains your dataset generation queries, index generation queries and queries used for running a benchmark.

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
    ("CREATE (:NodeA {id: 23});", {}),
    ("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" }),
    ...
]
```
As you can see you can pass just a Cypher query as pure string without any values in dictionary.

```python
("CREATE (:NodeA {id: 23});", {}),
```

Or you can specified parameters inside a dictionary, the variables next to `$` sign in Query string will be replaced by the appropriate values behind the key from the dictionary. In this case `$id` is replaced by `123` and `$property` is replaced by `foo`. The dictionary key names and variable names need to match.

```python
("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" })
```


Back to our `demo.py` example, here is the `dataset_generator()` method, here you actually want to specify queries for generating a dataset. In the first for loop the queries for creating 100 nodes with label `NodeA` and 100 nodes with label `NodeB` are prepared. Each node has `id` between 0 and 99. In the second for loop, queries for connecting nodes randomly are generated. There is total of 300 edges, each connected to random `NodeA` and `NodeB`.

```python
def dataset_generator(self):

    for i in range(0, 100):
        queries.append(("CREATE (:NodeA {id: $id});", {"id" : i}))
        queries.append(("CREATE (:NodeB {id: $id});", {"id" : i}))
    for i in range(0, 300):
        a = random.randint(0, 99)
        b = random.randint(0, 99)
        queries.append((("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id" : b}))

    return queries
```

### 4. Implement the index generator

The class should implement the `indexes_generator()` method. This is implemented the same way as `dataset_generator` class, instead of queries for dataset, `indexes_generator()` should return the list of indexes that will be used.


### 5. Define the queries you want to benchmark

Now that your dataset will be imported from dataset generator queries, you can specify what queries you wish to benchmark on the given dataset. Here are two queries that `demo.py` workload specifies, they are written as Python methods that returns a single tuple with query and dictionary, the same way as in data generator method.

```python
def benchmark__test__demo_query_get_nodes(self):
    return ("MATCH (n) RETURN n;", {})

def benchmark__test__demo_query_get_node_by_id(self):
    return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 99)})

```

The important details here are that each of the methods you wish to use in benchmark test needs to start with `benchmark__` in the name, otherwise it will be ignored.  The full method name has the following structure `benchmark__group__name`. Group can be used to execute specific tests, but more on that later.


From the workload setup perspective this is it.

Here are two more samples of simple demo workloads that are being generated via workload generator:

1. TODO
2. TODO


## How to run your custom workload

When running benchmarks, duration, query variety, number of workers play an important role next to the queries and parameters. MgBench provides several options on how to run benchmarks. Let's start with the simplest run of demo workload from above.

The main script that is managing benchmark execution is `benchmark.py`.

To start the benchmark you need to run the following command with your paths and options:

```python3 benchmark.py benchmarks demo/*/*/* --vendor-name (memgraphDocker||neo4jDocker) --export-results result.json  --no-authorization```

To run this on memgraph the command looks like this:

```python3 benchmark.py benchmarks demo/*/*/* --vendor-name memgraphDocker --export-results results.json --no-authorization```

Hopefully you should get a logs from `benchmark.py` process managing the benchmark. The script takes a lot of arguments, some used in the run above are self explanatory. But lets break down the most important ones:

- `NAME/VARIANT/GROUP/QUERY ` - The argument `demo/*/*/*` says execute workload for all workload, variants, group's an queries. This flag is used for a direct control what workload you wish to execute. The `NAME` here is the name of the workload defined in the Workload class. `VARIANT` is an additional configuration of the workload, will be explained a bit later. `GROUP` is defined in the query method name, and the `QUERY` is query name you wish to execute. If you wan't to execute specific query from `demo.py` it would look like this: `demo/*/test/get_nodes`. This will execute demo workload, all variants, test query group and query get_nodes.

- `--single-threaded-runtime-sec` - The question at hand is how many of each specific queries you wish to execute as a sample for a database benchmark. Each query can take a different time to execute, so fixating a number could yield some queries finishing in 1 second and other running for a minute. This flag defines the duration in seconds, that will yield how many of each specific queries you need for target duration of single thread runtime in seconds. Default value is 10 seconds, increasing this will yield a longer running test. Each query will get a count, that specify how many queries will be generated. This can be inspected after the test. For example  for 10 seconds of single threaded runtime, the queries from demo workload `get_node_by_id` got 64230 different queries, while `get_nodes` got 5061 because of different complexity.

- `--num-workers-for-benchmark` - The flag defines how many concurrent clients will open and query the database. With this flag you can simulate different database users connecting to database and executing queries. Each of the clients is independent and executing queries as fast as possible. They just share a  total pool of queries that were generated by the `--single-threaded-runtime-sec`.


- `--warm-up` - The warm-up flag can take a 3 different arguments, `cold`, `hot` and `vulcanic`. Cold is the default, there is no warm-up being executed, `hot` will execute some predefined queries before benchmark, while `vulcanic` will run the whole workload first, before taking measurements. Here is the implementation of [warm-up](https://github.com/memgraph/memgraph/blob/master/tests/mgbench/benchmark.py#L186)



## How to run the same workload on the different vendors

The base [Workload class](#1-inherit-the-workload-class) has an benchmarking context information that contains all benchmark arguments used in this run. Some are mentioned above. The key argument here is the `--vendor-name` which defines what database is being used in this benchmark.

During the creation of your workload, you can access the parent class property by using `self.benchmark_context.vendor_name`. For example if you want to specify special index creation for each vendor, the `indexes_generator()` could look like this:

```python
 def indexes_generator(self):
        indexes = []
        if "neo4j" in self.benchmark_context.vendor_name:
            indexes.extend(
                [
                    ("CREATE INDEX FOR (n:NodeA) ON (n.id);", {}),
                    ("CREATE INDEX FOR (n:NodeB) ON (n.id);", {}),
                ]
            )
        else:
            indexes.extend(
                [
                    ("CREATE INDEX ON :NodeA(id);", {}),
                    ("CREATE INDEX ON :NodeB(id);", {}),
                ]
            )
        return indexes
```

The same applies to the `dataset_generator()`.

## How to compare results


Once the benchmark has been run, the results are saved in file specified by `--export-results` argument. You can use the results files and compare them against other vendor results via the `compare_results.py` script:

```python compare_results.py --compare path_to/run_1.json path_to/run_2.json --output run_1_vs_run_2.html --different-vendors```

The output is an HTML file with the visual representation of the performance differences between two compared vendors. The first passed summary JSON file is the reference point. Feel free to open and HTML file in any browser at hand.
