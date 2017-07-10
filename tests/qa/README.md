# Memgraph quality assurance

In order to test dressipi's queries agains memgraph the following commands have
to be executed:
    1. ./init [Dxyz] # downloads query implementations + memgraph
                     # (the auth is manually for now) + optionally user can
                     # define arcanist diff which will be applied on the
                     # memgraph source code
    2. ./run         # compiles and runs database instance, also runs the
                     # test queries

TODO: automate further

## TCK Engine

Python script used to run tck tests against memgraph. To run script execute:

    1. python3 tck_engine/test_executor.py

Script uses Behave to run Cucumber tests.

The following tck tests have been changed:

    1. Tests where example injection did not work. Behave stores the first row
       in Cucumber tables as headings and the example injection is not working in
       headings. To correct this behavior, one row was added to tables where
       injection was used.

    2. Tests where the results were not always in the same order. Query does not
       specify the result order, but tests specified it. It led to the test failure.
       To correct tests, tag "the result should be" was changed with a
       tag "the result should be (ignoring element order for lists)".

    3. Behave can't escape character '|' and it throws parse error. Query was then
       changed and result was returned with different name.

Comparability.feature tests are failing because integers are compared to strings
what is not allowed in openCypher.

TCK Engine problems:

    1. Comparing tables with ordering.
         ORDER BY x DESC
         | x | y |    | x | y |
         | 3 | 2 |    | 3 | 1 |
         | 3 | 1 |    | 3 | 2 |
         | 1 | 4 |    | 1 | 4 |

    2. Properties side effects
         | +properties | 1 |
         | -properties | 1 |

         Database is returning properties_set, not properties_created and properties_deleted.
