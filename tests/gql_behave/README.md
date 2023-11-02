# Memgraph GQL Behave Tests

Python script used to run graph query language behavior tests against Memgraph.

To run the script please execute:
```
cd memgraph/tests/gql_behave
source ve3/bin/activate
./run.py --help
./run.py memgraph_V1
```

The script requires one positional parameter that specifies which test suite
should be executed. All available test suites can be found in the `tests/`
directory.

## Graph Sizes

IMPORTANT: Please prepare small graphs, up to 1000 nodes+edges because this
engine is not optimized to run on a huge graphs. Furthermore, semantics should
always be tested on a small scale.

## openCypher TCK Tests

The script uses Behave to run Cucumber tests.

Some gotchas exist when adding openCypher TCK tests to the engine:

 - In some tests example injection did not work. Behave stores the first row in
   Cucumber tables as headings and the example injection failed to work.  To
   correct this behavior, one row was added to tables where injection was used.

 - Some tests don't have fully defined result ordering. Because the tests rely
   on result order, some tests fail. If you find a flaky test to ignore output
   ordering you should change the tag "the result should be" to "the result
   should be (ignoring element order for lists)".

 - Behave can't escape character '|' and it throws a parse error. The query was
   then changed and the result was returned with a different name.

`Comparability.feature` tests are failing because integers are compared to
strings what is not allowed in openCypher.

## The Engine Issues

Comparing tables with ordering doesn't always work, example:
```
ORDER BY x DESC
| x | y |    | x | y |
| 3 | 2 |    | 3 | 1 |
| 3 | 1 |    | 3 | 2 |
| 1 | 4 |    | 1 | 4 |
```

Side effect aren't tracked or verified, example:
```
| +properties | 1 |
| -properties | 1 |
```
This is because Memgraph currently doesn't give out the list of side effects
that happend during query execution.
