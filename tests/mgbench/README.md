# :fire: mgBench: Benchmark for graph databases

## :clipboard: Benchmark Overview

mgBench is primarily designed to benchmark graph databases (Currently, Neo4j and Memgraph). To test graph database performance, this benchmark executes Cypher queries that can write, read, update, aggregate, and analyze dataset present in database. There are some predefined queries and dataset in mgbench. The present datasets and queries represent a typical workload that would be used to analyze any graph dataset and are pure Cypher based. [BenchGraph](https://memgraph.com/benchgraph/) platform shows the results of running these queries on specified hardware and under certain conditions. It shows the overall performance of each system under test relative to other, best in test being the baseline.

There is also a [tutorial on how to use mgbench](how_to_use_mgbench.md) to define your own workload and run workloads on supported vendors.

Mgbench supports three workload types can be executed:
- Isolated - Concurrent execution of a single type of query,
- Mixed - Concurrent execution of a single type of query mixed with a certain percentage of queries from a designated query group,
- Realistic - Concurrent execution of queries from write, read, update and analyze groups.

This methodology is designed to be read from top to bottom to understand what is being tested and how, but feel free to jump to parts that interest you.

- [:fire: mgBench: Benchmark for graph databases](#fire-mgbench-benchmark-for-graph-databases)
  - [:clipboard: Benchmark Overview](#clipboard-benchmark-overview)
  - [:dart: Design goals](#dart-design-goals)
    - [Reproducibility and validation](#reproducibility-and-validation)
    - [Database compatibility](#database-compatibility)
    - [Workloads](#workloads)
    - [Fine-tuning](#fine-tuning)
    - [Limitations](#limitations)
  - [:wrench: mgBench](#wrench-mgbench)
    - [Important files](#important-files)
    - [Prerequisites](#prerequisites)
    - [Running the benchmark](#running-the-benchmark)
    - [Database conditions](#database-conditions)
    - [Comparing results](#comparing-results)
  - [:bar\_chart: Results](#bar_chart-results)
  - [:books: Datasets](#books-datasets)
    - [Pokec](#pokec)
      - [Query list](#query-list)
  - [:computer: Platform](#computer-platform)
    - [Intel - HP](#intel---hp)
  - [:nut\_and\_bolt: Supported databases](#nut_and_bolt-supported-databases)
    - [Database notes](#database-notes)
  - [:raised\_hands: Contributions](#raised_hands-contributions)
  - [:mega: History and Future of mgBench](#mega-history-and-future-of-mgbench)
    - [History of mgBench](#history-of-mgbench)
    - [Future of mgBench](#future-of-mgbench)

## :dart: Design goals

### Reproducibility and validation

Running this benchmark is automated, and the code used to run benchmarks is publicly available. You can  [run mgBench](#running-the-benchmark) with default settings to validate the results at [BenchGraph platform](https://memgraph.com/benchgraph). The results may differ depending on the hardware, benchmark run configuration, database configuration, and other variables involved in your setup.  But if the results you get are significantly different, feel free to [open a GitHub issue](https://github.com/memgraph/memgraph/issues).

In the future, the project will be expanded to include more platforms to see how systems perform on different OS and hardware configurations. If you are interested in what will be added and tested, read the section about [the future of mgBench](#future-of-mgbench)


### Database compatibility

At the moment, support for graph databases is limited. To run the benchmarks, the graph database must support Cypher query language and the Bolt protocol.

Using Cypher ensures that executed queries are identical or similar as possible on every supported system. A single C++ client queries database systems (Currently, Neo4j and Memgraph), and it is based on the Bolt protocol. Using a single client ensures minimal performance penalties from the client side and ensures fairness across different vendors.

If your database supports the given requirements, feel free to contribute and add your database to mgBench.
If your database does not support the mentioned requirements, follow the project because support for other languages and protocols in graph database space will be added.


### Workloads
Running queries as standalone units is simple and relatively easy to measure, but vendors often apply various caching and pre-aggregations that influence the results in these kinds of scenarios.  Results from running single queries can hint at the database's general performance, but in real life, a database is queried by multiple clients from multiple sides. That is why the mgBench client supports the consecutive execution of various queries. Concurrently writing, reading, updating and executing aggregational and analytical queries provides a better view of overall system performance than executing and measuring a single query. Queries that the mgBench executes are grouped into 5 groups - write, read, update, aggregate and analytical.

The [BenchGraph platform](https://memgraph.com/benchgraph) shows results made by mgBench by executing three types of workloads:
- ***Isolated workload***
- ***Mixed workload***
- ***Realistic workload***

Each of these workloads has a specific purpose:

***Isolated*** workload is the simplest test. An isolated workload goes through all the queries individually, concurrently executing a single query a predefined number of times. It is similar to executing a single query and measuring time but more complex due to concurrency. How many times a specific query will be executed depends on the approximation of the query’s latency. If a query is slower, it will be executed fewer times, if a query is faster, it will be executed more times. The approximation is based on the duration of execution for several concurrent threads, and it varies between vendors.
If a query takes arguments, the argument value is changed for each execution. Arguments are generated non-randomly, so each vendor gets the same sequence of queries with the same arguments. This enables a deterministic workload for both vendors.
The good thing about isolated workload is that it yields a better picture of single query performance. There is also a negative side, executing the same queries multiple times can trigger strong results caching on the vendor's side, which can result in false query times.


***Mixed*** workload executes a fixed number of queries that read, update, aggregate, or analyze the data concurrently with a certain percentage of write queries because writing from the database can prevent aggressive caching and thus represent a more realistic performance of a single query. The negative side is that there is an added influence of write performance on the results. Currently, mgBench client does not support per-thread performance measurements, but this will be added in future iterations.


***Realistic*** workload represents real-life use cases because queries write, read, update, and perform analytics in a mixed ratio like they would in real projects. The test executes a fixed number of queries, the distribution of which is defined by defining a percentage of queries performing one of four operations. The queries are selected non-randomly, so the workload is identical between different vendors. As with the rest of the workloads, all queries are executed concurrently.

### Fine-tuning

Each database system comes with a wide variety of possible configurations. Changing each of those configuration settings can introduce performance improvements or penalties. The focus of this benchmark is "out-of-the-box" performance without fine-tuning with the goal of having the fairest possible comparison. Fine-tuning can make some systems perform magnitudes faster, but this makes general benchmark systems hard to manage because all systems are configured differently, and fine-tuning requires vendor DB experts.

Some configurational changes are necessary for test execution and are not considered fine-tuning. For example, configuring the database to avoid Bolt client login is valid since the tests are not performed under any type of authorization. All non-default configurations are mentioned in [database notes](#database-notes)

### Limitations

Benchmarking different systems is challenging because the setup, environment, queries, workload, and dataset can benefit specific database vendors. Each vendor may have a particularly strong use-case scenario. This benchmark aims to be neutral and fair to all database vendors. Acknowledging some of the current limitations can help understand the issues you might notice:
1. mgBench measures and tracks just a tiny subset of everything that can be tracked and compared during testing. Active benchmarking is strenuous because it requires a lot of time to set up and validate. Passive benchmarking is much faster to iterate on but can have a few bugs.
2. The scale of the dataset used is miniature for production environments. Production environments can have up to trillions of nodes and edges.
Query results are not verified or important. The queries might return different results, but only the performance is measured, not correctness.
3. All tests are performed on single-node databases.
4. Architecturally different systems can be set up and measured biasedly.


## :wrench: mgBench
### Important files

Listed below are the main scripts used to run the benchmarks:

- `benchmark.py` - Script that runs the single workload iteration of queries.
- `workloads.py` - Base script that defines how dataset and queries are defined.
- `runners.py` - Script holding the configuration for different DB vendors.
- `client.cpp` - Client for querying the database.
- `graph_bench.py` - Script that starts all predefined and custom-defined workloads thst.
- `compare_results.py` - Script that visually compares benchmark results.

Except for these scripts, the project also includes dataset files and index configuration files. Once the first test is executed, those files can be located in the newly generated `.cache` folder.

### Prerequisites

To execute a benchmark, you need to download a binary version of supported databases and install Python on your system. Each database vendor can depend on external dependencies, such as Cmake, JVM, etc., so make sure to check specific vendor prerequisites.

### Running the benchmark
To run benchmarks, use the `graph_bench.py` script, which calls all the other necessary scripts. You can start the benchmarks by executing the following command:

```
graph_bench.py
--vendor memgraph /home/memgraph/binary
--dataset-group basic
--dataset-size small
--realistic 100 30 70 0 0
--realistic 100 50 50 0 0
--realistic 100 70 30 0 0
--realistic 100 30 40 10 20
--mixed 100 30 0 0 0 70
```


Isolated workload are always executed, and this commands calls for the execution of four realistic workloads with different distribution of queries and one mixed workload on a small size dataset.

The distribution of queries from write, read, update and aggregate groups are defined in percentages and stated as arguments following the `--realistic` or `--mixed` flags.

In the example of `--realistic 100 30 40 10 20` the distribution is as follows:

- 100 - The number of queries to be executed.
- 30 - The percentage of write queries to be executed.
- 40 - The percentage of read queries to be executed.
- 10 - The percentage of update queries to be executed.
- 20 - The percentage of analytical queries to be executed.


For `--mixed` workload argument, the first five parameters are the same, with an addition of a parameter for defining the percentage of individual queries.

Feel free to add different configurations if you want. Results from the above benchmark run are visible on [BenchGraph platform](https://memgraph.com/benchgraph)

### Database conditions
In a production environment, database query caches are usually warmed from usage or pre-warm procedure to provide the best possible performance. Each workload in mgBench will be executed under the following conditions:
- ***Hot run*** - before executing any benchmark query and taking measurements, a set of defined queries is executed to pre-warm the database.
- ***Cold run*** - no warm-up was performed on the database before taking benchmark measurements.

List of queries used for pre-warm up:
```
CREATE ();
CREATE ()-[:TempEdge]->();
MATCH (n) RETURN n LIMIT 1;
```

### Comparing results

Once the benchmark has been run for a single vendor, all the results are saved in appropriately named `.json` files. A summary file is also created for that vendor and it contains all results combined. These summary files are used to compare results against other vendor results via the `compare_results.py` script:

```
compare_results.py
--compare
“path_to/neo4j_summary.json”
“path_to/memgraph_summary.json”
--output neo4j_vs_memgraph.html
--different-vendors
```

The output is an HTML file with the visual representation of the performance differences between two compared vendors. The first passed summary JSON file is the reference point.

## :bar_chart: Results
Results visible in the HTML file or at [BenchGraph](https://memgraph.com/benchgraph) are throughput, memory, and latency. Database throughput and memory usage directly impact database usability and cost, while the latency of the query shows the base query execution duration.

***Throughput*** directly defines how performant the database is and how much query traffic it can handle in a fixed time interval. It is expressed in queries per second. In each concurrent workload, execution is split across multiple clients. Each client executes queries concurrently. The duration of total execution is the sum of all concurrent clients' execution duration in seconds. In mgBench, the total count of executed queries and the total duration defines throughput per second across concurrent execution.

Here is the code snippet from the client, that calculates ***throughput*** and metadata:
```
  // Create and output summary.
  Metadata final_metadata;
  uint64_t final_retries = 0;
  double final_duration = 0.0;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    final_metadata += worker_metadata[i];
    final_retries += worker_retries[i];
    final_duration += worker_duration[i];
  }
  final_duration /= FLAGS_num_workers;
  nlohmann::json summary = nlohmann::json::object();
  summary["count"] = queries.size();
  summary["duration"] = final_duration;
  summary["throughput"] = static_cast<double>(queries.size()) / final_duration;
  summary["retries"] = final_retries;
  summary["metadata"] = final_metadata.Export();
  summary["num_workers"] = FLAGS_num_workers;
  (*stream) << summary.dump() << std::endl;
```

***Memory*** usage is calculated as ***peak RES*** (resident size) memory for each query or workload execution within mgBench. The result includes starting the database, executing the query/workload, and stopping the database. The peak RES is extracted from process PID as VmHVM (peak resident set size) before the process is stopped. The peak memory usage defines the worst-case scenario for a given query or workload, while on average, RAM footprint is lower. Measuring RES over time is supported by `runners.py`. For each vendor, it is possible to add RES tracking across workload execution, but it is not reported in the results.
***Latency*** is calculated as the serial execution of 100 identical queries on a single thread. Each query has standard query statistics and tail latency data. The result includes query execution times: max, min, mean, p99, p95, p90, p75, and p50 in seconds.

Each workload and all the results are based on concurrent query execution, except ***latency***.  As stated in [limitations](#limitations) section, mgBench tracks just a subset of resources, but the chapter on [mgBench future](#future-of-mgbench) explains the expansion plans.

## :books: Datasets
Before workload execution, appropriate dataset indexes are set.  Each vendor can have a specific syntax for setting up indexes, but those indexes should be schematically as similar as possible.

After each workload is executed, the database is cleaned, and a new dataset is imported to provide a clean start for the following workload run. When executing isolated and mixed workloads, the database is also restarted after executing each query to minimize the impact on the following query execution.

### Pokec

Currently, the only available dataset to run the benchmarks on is the Slovenian social network, Pokec. It’s available in three different sizes, small, medium, and large.
- [small](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher) - vertices 10,000, edges 121,716
- [medium](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.cypher) - vertices 100,000, edges 1,768,515
- [large](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_large.setup.cypher.gz) - vertices 1,632,803, edges 30,622,564.

Dataset is imported as a CYPHERL file of Cypher queries. Feel free to check dataset links for complete Cypher queries.
Once the script is started, a single index is configured on (:User{id}). Only then are queries executed.
Index queries for each supported vendor can be downloaded from “https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/vendor_name.cypher”, just make sure to use the proper vendor name such as `memgraph.cypher`.

#### Query list

| |Name     | Group | Query |
|-|-----| -- | ------------ |
|Q1|aggregate | aggregate | MATCH (n:User) RETURN n.age, COUNT(*)|
|Q2|aggregate_count | aggregate | MATCH (n) RETURN count(n), count(n.age)|
|Q3|aggregate_with_filter | aggregate | MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)|
|Q4|min_max_avg | aggregate | MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)|
|Q5|expansion_1 | analytical | MATCH (s:User {id: $id})-->(n:User) RETURN n.id|
|Q6|expansion_1_with_filter| analytical | MATCH (s:User {id: $id})-->(n:User) WHERE n.age >= 18 RETURN n.id|
|Q7|expansion_2| analytical | MATCH (s:User {id: $id})-->()-->(n:User) RETURN DISTINCT n.id|
|Q8|expansion_2_with_filter| analytical | MATCH (s:User {id: $id})-->()-->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id|
|Q9|expansion_3| analytical | MATCH (s:User {id: $id})-->()-->()-->(n:User) RETURN DISTINCT n.id|
|Q10|expansion_3_with_filter| analytical | MATCH (s:User {id: $id})-->()-->()-->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id|
|Q11|expansion_4| analytical | MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) RETURN DISTINCT n.id|
|Q12|expansion_4_with_filter| analytical | MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id|
|Q13|neighbours_2| analytical | MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id|
|Q14|neighbours_2_with_filter| analytical | MATCH (s:User {id: $id})-[*1..2]->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id|
|Q15|neighbours_2_with_data| analytical | MATCH (s:User {id: $id})-[*1..2]->(n:User) RETURN DISTINCT n.id, n|
|Q16|neighbours_2_with_data_and_filter| analytical | MATCH (s:User {id: $id})-[*1..2]->(n:User) WHERE n.age >= 18 RETURN DISTINCT n.id, n|
|Q17|pattern_cycle| analytical | MATCH (n:User {id: $id})-[e1]->(m)-[e2]->(n) RETURN e1, m, e2|
|Q18|pattern_long| analytical | MATCH (n1:User {id: $id})-[e1]->(n2)-[e2]->(n3)-[e3]->(n4)<-[e4]-(n5) RETURN n5 LIMIT 1|
|Q19|pattern_short| analytical | MATCH (n:User {id: $id})-[e]->(m) RETURN m LIMIT 1|
|Q20|single_edge_write| write | MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m CREATE (n)-[e:Temp]->(m) RETURN e|
|Q21|single_vertex_write| write |CREATE (n:UserTemp {id : $id}) RETURN n|
|Q22|single_vertex_property_update| update | MATCH (n:User {id: $id}) SET n.property = -1|
|Q23|single_vertex_read| read | MATCH (n:User {id : $id}) RETURN n|

## :computer: Platform

Testing on different hardware platforms and cloudVMs is essential for validating benchmark results. Currently, the tests are run on two different platforms.

### Intel - HP

- Server: HP DL360 G6
- CPU: 2 x Intel Xeon X5650 6C12T @ 2.67GHz
- RAM: 144GB
- OS: Debian 4.19

## :nut_and_bolt: Supported databases

Due to current [database compatibility](link) requirements, the only supported database systems at the moment are:
1. Memgraph v2.4
2. Neo4j Community Edition v5.1.

### Database notes

Running configurations that differ from default configuration:

- Memgraph - `storage_snapshot_on_exit=true`, `storage_recover_on_startup=true`
- Neo4j - `dbms.security.auth_enabled=false`

## :raised_hands: Contributions

As previously stated, mgBench will expand, and we will need help adding more datasets, queries, databases, and support for protocols in mgBench. Feel free to contribute to any of those, and throw us a start :star:!

## :mega: History and Future of mgBench
### History of mgBench

Infrastructure around mgBench was developed to test and maintain Memgraph performance. When critical code is changed, a performance test is run on Memgraph’s CI/CD infrastructure to ensure performance is not impacted. Due to the usage of mgBench for internal testing, some parts of the code are still tightly connected to Memgraph’s CI/CD infrastructure. The remains of that code do not impact benchmark setup or performance in any way.

### Future of mgBench
We have big plans for mgBench infrastructure that refers to the above mentioned [limitations](#limitations). Even though a basic dataset can give a solid indication of performance, adding bigger and more complex datasets is a priority to enable the execution of complex analytical queries.

Also high on the list is expanding the list of vendors and providing support for different protocols and languages. The goal is to use mgBench to see how well Memgraph performs on various benchmarks tasks and publicly commit to improving.

mgBench is currently a passive benchmark since resource usage and saturation across execution are not tracked. Sanity checks were performed, but these values are needed to get the full picture after each test. mgBench also deserves its own repository, and it will be decoupled from Memgraph’s testing infrastructure.
