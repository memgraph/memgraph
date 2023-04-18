# How to use mgBench

Running your workloads that include custom queries and the dataset is the best way to evaluate system performance on your use case. Each workload has unique requirements that are imposed from the use case. Since your use-case queries and dataset will be used in production, it is best to use those.
We worked on cleaning MgBench architecture so it is easier for users to add their custom workloads and queries to evaluate performance on supported systems.

This tutorial contains the following content:

- [How to add your custom workload](#how-to-add-your-custom-workload)
- [How to run benchmarks on your custom workload](#how-to-run-benchmarks-on-your-custom-workload)
- [How to configure benchmark run](#how-to-configure-benchmark-run)
- [How to compare results](#how-to-compare-results)
- [Customizing workload generator](#customizing-workload-generator)


## How to add your custom workload

If you want to run your custom workload on supported systems (Currently, Memgraph and Neo4j), you can start by writing a simple Python class. The idea is to specify a simple class that contains your dataset generation queries, index generation queries and queries used for running a benchmark.

Here are 5 steps you need to do to specify your **workload**:

1. [Inherit the workload class](#1-inherit-the-workload-class)
2. [Define a workload name](#2-define-the-workload-name)
3. [Implement dataset generator method](#3-implement-dataset-generator-method)
4. [Implement index generator method](#4-implement-the-index-generator)
5. [Define the queries you want to benchmark](#4-define-the-queries-you-want-to-benchmark)

Here is the simplified version of [demo.py](https://github.com/memgraph/memgraph/blob/master/tests/mgbench/workloads/demo.py) example:

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

Let's break this script down into smaller important elements:

### 1. Inherit the `workload` class
The `Demo` script class has a parent class `Workload`. Each custom workload should inherit from the base `Workload` class.

```python
from workloads.base import Workload

class Demo(Workload):
```

### 2. Define the workload name
The class should specify the `NAME` property. This is used to describe what workload class you want to execute. When calling `benchmark.py`, this property will be used to differentiate different workloads.

```python
NAME = "demo"
```

### 3. Implement dataset generator method
The class should implement the `dataset_generator()` method. The method generates a dataset that returns the ***list of tuples***. Each tuple contains a string of the Cypher query and dictionary that contains optional arguments, so the structure is following [(str, dict), (str, dict)...]. Let's take a look at how the example list could look like what it could method return:

```python
queries = [
    ("CREATE (:NodeA {id: 23});", {}),
    ("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" }),
    ...
]
```
As you can see, you can pass just a Cypher query as a pure string without any values in the dictionary.

```python
("CREATE (:NodeA {id: 23});", {}),
```

Or you can specify parameters inside a dictionary. The variables next to `$` sign in the query string will be replaced by the appropriate values behind the key from the dictionary. In this case `$id` is replaced by `123` and `$property` is replaced by `foo`. The dictionary key names and variable names need to match.

```python
("CREATE (:NodeB {id: $id, foo: $property});", {"id" : 123, "property": "foo" })
```

Back to our `demo.py` example, in the `dataset_generator()` method, here you specify queries for generating a dataset. In the first for loop the queries for creating 100 nodes with the label `NodeA` and 100 nodes with the label `NodeB` are prepared. Each node has `id` between 0 and 99. In the second for loop, queries for connecting nodes randomly are generated. There is a total of 300 edges, each connected to random `NodeA` and `NodeB`.

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

### 4. Implement the index generator method

The class should also implement the `indexes_generator()` method. This is implemented the same way as the `dataset_generator()` method, instead of queries for the dataset, `indexes_generator()` should return the list of indexes that will be used. The returning structure again is the list of tuples that contains query string and dictionary of parameters. Here is an example:

```python
def indexes_generator(self):
    indexes = [
                ("CREATE INDEX ON :NodeA(id);", {}),
                ("CREATE INDEX ON :NodeB(id);", {}),
            ]
    return indexes
```

### 5. Define the queries you want to benchmark

Now that your dataset will be imported from dataset generator queries, you can specify what queries you wish to benchmark on the given dataset. Here are two queries that `demo.py` workload defines. They are written as Python methods that return a single tuple with query and dictionary, as in the data generator method.

```python
def benchmark__test__get_nodes(self):
    return ("MATCH (n) RETURN n;", {})

def benchmark__test__get_node_by_id(self):
    return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 99)})

```

The necessary details here are that each of the methods you wish to use in the benchmark test needs to start with `benchmark__` in the name, otherwise, it will be ignored. The complete method name has the following structure `benchmark__group__name`. The group can be used to execute specific tests, but more on that later.

From the workload setup, this is all you need to do. Next step is how to run your workload. If you wish to improve the workload generator, look at [customizing workload generator](#customizing-workload-generator).

## How to run benchmarks on your custom workload

When running benchmarks, duration, query arguments, number of workers, and database condition play an important role on the results of the benchmark. MgBench provides several options for the configuration of how the benchmark is executed. Let's start with the most straightforward run of the demo workload from the example above.

The main script that manages benchmark execution is `benchmark.py`.

To start the benchmark, you need to run the following command with your paths and options:

```python3 benchmark.py vendor-docker --vendor-name (memgraph-docker||neo4j-docker) benchmarks demo/*/*/* --export-results result.json  --no-authorization```

To run this on memgraph, the command looks like this:

```python3 benchmark.py vendor-docker --vendor-name memgraph-docker benchmarks demo/*/*/* --export-results results.json --no-authorization```

## How to configure benchmark run

Hopefully, you should get logs from `benchmark.py` process managing the benchmark and execution from the command above. The script takes a lot of arguments. Some used in the run above are self-explanatory. But let's break down the most important ones:

- `NAME/VARIANT/GROUP/QUERY ` - The argument `demo/*/*/*` says to execute the workload named `demo`, and all of its variants, group's and queries. This flag is used for direct control of what workload you wish to execute. The `NAME` here is the name of the workload defined in the Workload class. `VARIANT` is an additional workload configuration, which will be explained a bit later. `GROUP` is defined in the query method name, and the `QUERY` is query name you wish to execute. If you want to execute a specific query from `demo.py`, it would look like this: `demo/*/test/get_nodes`. This will run `demo` workload on all `variants`, in `test` query group and query `get_nodes`.

- `--single-threaded-runtime-sec` - The question at hand is how many of each specific queries you wish to execute as a sample for a database benchmark. Each query can take a different time to execute, so fixating a number could yield some queries finishing in 1 second and others running for a minute. This flag defines the duration in seconds that will be used to approximate how many queries you wish to execute. The default value is 10 seconds, this means the `benchmark.py` will generate predetermined numbers of queries to approximate single treaded runtime of 10 seconds. Increasing this will yield a longer running test.
Each specific query will get a different count that specifies how many queries will be generated. This can be inspected after the test. For example, for 10 seconds of single-threaded runtime, the queries from demo workload `get_node_by_id` got 64230 different queries, while `get_nodes` got 5061 because of different time complexity of queries.

- `--num-workers-for-benchmark` - The flag defines how many concurrent clients will open and query the database. With this flag, you can simulate different database users connecting to the database and executing queries. Each of the clients is independent and executes queries as fast as possible. They share a total pool of queries that were generated by the `--single-threaded-runtime-sec`. This means the total number of queries that need to be executed is shared between a specified number of workers.

- `--warm-up` - The warm-up flag can take a three different arguments, `cold`, `hot` and `vulcanic`. Cold is the default. There is no warm-up being executed, `hot` will execute some predefined queries before the benchmark, while `vulcanic` will run the whole workload first before taking measurements. Here is the implementation of [warm-up](https://github.com/memgraph/memgraph/blob/master/tests/mgbench/benchmark.py#L186)


## How to compare results


Once the benchmark has been run, the results are saved in a file specified by `--export-results` argument. You can use the results files and compare them against other vendor results via the `compare_results.py` script:

```python compare_results.py --compare path_to/run_1.json path_to/run_2.json --output run_1_vs_run_2.html --different-vendors```

The output is an HTML file with a visual representation of the performance differences between two compared vendors. The first passed summary JSON file is the reference point. Feel free to open an HTML file in any browser at hand.

## Customizing workload generator

### How to run the same workload on the different vendors

The base [Workload class](#1-inherit-the-workload-class) has benchmarking context information that contains all benchmark arguments used in this run. Some are mentioned above. The key argument here is the `--vendor-name`, which defines what database is being used in this benchmark.

During the creation of your workload, you can access the parent class property by using `self.benchmark_context.vendor_name`. For example, if you want to specify special index creation for each vendor, the `indexes_generator()` could look like this:

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

The same applies to the `dataset_generator()`. During the generation of the dataset, you can use special types of queries for different vendors.
