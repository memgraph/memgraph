# LDBC benchmarks

## How to run the benchmark against Neo4j OR Memgraph?

    cd memgraph/tests/public_benchmark/ldbc
    ./setup
    ./build_dataset [--scale-factor 1]
    # To run short reads by default, just call:
    ./run_benchmark --create-index --run-db memgraph # or neo4j
    # To run update queries pass the properties file for updates and slow down
    # the execution by setting a larger time compression ratio.
    ./run_benchmark --create-index --run-db memgraph --test-type updates \
                    --time-compression-ratio 1.5

## How to run a specific test?

    cd memgraph/tests/public_benchmark/ldbc/ldbc-snb-impls/snb-interactive-neo4j
    mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.neo4j.util.QueryTester" -Dexec.args="shortquery1 933 --repeat=10000 --timeUnits=MICROSECONDS"

## Useful terminal commands

### How to find out Default Query Limits?

    cd memgraph/tests/public_benchmark/ldbc/ldbc_driver
    ag "DEFAULT_LIMIT"

## How to run test cases?

    cd memgraph/tests/public_benchmark/ldbc
    neo4j-client --insecure -u "" -p "" -o test_cases/results/short_query_2.oc.out -i test_cases/queries/short_query_2.oc localhost 7687

## How to create indexes manually?

    cd memgraph/tests/public_benchmark/ldbc
    source ve3/bin/activate
    ./index_creation ldbc-snb-impls/snb-interactive-neo4j/scripts/indexCreation.neo4j

## Where is and how to use LDBC plotting?

    cd memgraph/tests/public_benchmark/ldbc
    source ve2/bin/activate
    cd memgraph/tests/public_benchmark/ldbc/ldbc_driver/plotting
    python make_charts_all_queries_all_metrics.py /path/to/ldbc_driver/results/LDBC-results.json legend_location

## TODOs

 1. Run & validate all Short queries + optimize MemGraph performance.
 2. Write the log parser.
 3. Visualize the results.
 4. Run & validate all Update queries + optimize MemGraph performance.
 5. Run apacaci code an compare the results (Do We Need Specialized Graph Database?).
     * https://github.com/anilpacaci/ldbc_driver
     * https://github.com/anilpacaci/ldbc_snb_implementations
     * https://github.com/anilpacaci/ldbc-snb-impls
     * https://github.com/anilpacaci/graph-benchmarking
 6. Optimize MemGraph performance.
 7. Scale benchmarks (scale factors 1, 3, 10).
 8. Test against Postgres and make comparison between Neo, Memgraph & Postgres. Latency + Throughput.
 9. Run & validate all queries + optimize MemGraph performance.
10. Optimize MemGraph spinup time.
