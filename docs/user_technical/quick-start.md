## Quick Start

This chapter outlines several ways to execute openCypher queries on Memgraph,
but first it's important to outline several technologies Memgraph uses.

### OpenCypher

Memgraph supports the openCypher query language which has been developed by
[Neo4j](http://neo4j.com). The language is currently going through a
vendor-independent standardization process. It's a declarative language
developed specifically for interaction with graph databases.

### Multiple-Query Transactions

Memgraph supports transactions containing multiple queries. In other words,
one can set an explicit transaction start and stop. All of the queries inside
will be executed as a single transaction. This means that if a single query
execution fails, all of the executed queries will be reverted and the
transaction aborted.

### Bolt

Clients connect to Memgraph using the
[Bolt protocol](https://boltprotocol.org/). Bolt was designed for efficient
communication with graph databases. Memgraph supports
[Version 1](https://boltprotocol.org/v1/) of the protocol. Official Bolt
protocol drivers are provided for multiple programming languages:

  * [Java](http://neo4j.com/docs/api/java-driver)
  * [Python](http://neo4j.com/docs/api/python-driver)
  * [JavaScript](http://neo4j.com/docs/api/javascript-driver)
  * [C#](http://neo4j.com/docs/api/dotnet-driver)

It's also possible to interact with Memgraph using Neo4j's command-line tool,
which is the easiest way for executing openCypher queries on Memgraph.

### Graph Gists

A nice looking set of small graph examples could be found
[here](https://neo4j.com/graphgists/).  You can take any use-case and try to
execute the queries against Memgraph. We welcome your feedback!

### neo4j-client Example

The command-line neo4j-client can be installed as described
[on the official website](https://neo4j-client.net).

The client can be started and connected to Memgraph with the following
shell command:

```
neo4j-client bolt://<IP_ADDRESS>:<PORT> --insecure --user u --pass p
```

Where `<IP_ADDRESS>` and `<PORT>` should be replaced with the network location
where Memgraph is reachable. The `--insecure` option specifies that SLL should
be disabled (Memgraph currently does not support SSL).  `--user` and `--pass`
parameter values are ignored by Memgraph (currently the database is
single-user), but need to be provided for the client to connect automatically.

After the client has started it should present a command prompt similar to:

```
neo4j-client 2.1.3
Enter `:help` for usage hints.
Connected to 'neo4j://neo@127.0.0.1:7687' (insecure)
neo4j>
```


At this point it is possible to execute openCypher queries on Memgraph. Each
query needs to end with the `;` (*semicolon*) character. For example:

```
CREATE (u:User {name: "Your Name"})-[:Likes]->(m:Software {name: "Memgraph"});
```

followed by:

```
MATCH (u:User)-[r]->(x) RETURN u, r, x;
```

### Python Driver Example

Neo4j officially supports Python for interacting with an openCypher and Bolt
compliant database. For details consult the
[official documentation](http://neo4j.com/docs/api/python-driver) and the
[GitHub project](https://github.com/neo4j/neo4j-python-driver).  Following is
a basic usage example:

```
from neo4j.v1 import GraphDatabase, basic_auth

# Initialize and configure the driver.
#   * provide the correct URL where Memgraph is reachable;
#   * use an empty user name and password, and
#   * disable encryption (not supported).
driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=basic_auth("", ""),
                              encrypted=False)

# Start a session in which queries are executed.
session = driver.session()

# Execute openCypher queries.
# After each query, call either `consume()` or `data()`
session.run('CREATE (alice:Person {name: "Alice", age: 22})').consume()

# Get all the vertices from the database (potentially multiple rows).
vertices = session.run('MATCH (n) RETURN n').data()
# Assuming we started with an empty database, we should have Alice
# as the only row in the results.
only_row = vertices.pop()
alice = only_row["n"]

# Print out what we retrieved.
print("Found a vertex with labels '{}', name '{}' and age {}".format(
  alice['name'], alice.labels, alice['age'])

# Remove all the data from the database.
session.run('MATCH (n) DETACH DELETE n').consume()

# Close the session and the driver.
session.close()
driver.close()
```

### Java Driver Example

The details about Java driver can be found
[on GitHub](https://github.com/neo4j/neo4j-java-driver).

The example below is equivalent to Python example. Major difference is that
`Config` object has to be created before the driver construction.  Encryption
has to be disabled by calling `withoutEncryption` method against the `Config`
builder.

```
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.*;
import static org.neo4j.driver.v1.Values.parameters;
import java.util.*;

public class JavaQuickStart {
    public static void main(String[] args) {
        // Initialize driver.
        Config config = Config.build().withoutEncryption().toConfig();
        Driver driver = GraphDatabase.driver("bolt://localhost:7687",
                                             AuthTokens.basic("",""),
                                             config);
        // Execute basic queries.
        try (Session session = driver.session()) {
            StatementResult rs1 = session.run("MATCH (n) DETACH DELETE n");
            StatementResult rs2 = session.run(
                "CREATE (alice: Person {name: 'Alice', age: 22})");
            StatementResult rs3 = session.run( "MATCH (n) RETURN n");
            List<Record> records = rs3.list();
            Record record = records.get(0);
            Node node = record.get("n").asNode();
            System.out.println(node.get("name").asString());
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        // Cleanup.
        driver.close();
    }
}
```

### Javascript Driver Example

The details about Javascript driver can be found
[on GitHub](https://github.com/neo4j/neo4j-javascript-driver).

The Javascript example below is equivalent to Python and Java examples. SSL
can be disabled by passing `{encrypted: 'ENCRYPTION_OFF'}` during the driver
construction.

Here is an example related to `Node.js`. Memgraph doesn't have integrated
support for `WebSocket` which is required during the execution in any web
browser. If you want to run `openCypher` queries from a web browser,
[websockify](https://github.com/novnc/websockify) has to be up and running.
Requests from web browsers are wrapped into `WebSocket` messages, and a proxy
is needed to handle the overhead. The proxy has to be configured to point out
to Memgraph's Bolt port and web browser driver has to send requests to the
proxy port.

```
var neo4j = require('neo4j-driver').v1;
var driver = neo4j.driver("bolt://localhost:7687",
                          neo4j.auth.basic("neo4j", "1234"),
                          {encrypted: 'ENCRYPTION_OFF'});
var session = driver.session();

function die() {
  session.close();
  driver.close();
}

function run_query(query, callback) {
  var run = session.run(query, {});
  run.then(callback).catch(function (error) {
    console.log(error);
    die();
  });
}

run_query("MATCH (n) DETACH DELETE n", function (result) {
  console.log("Database cleared.");
  run_query("CREATE (alice: Person {name: 'Alice', age: 22})", function (result) {
    console.log("Record created.");
    run_query("MATCH (n) RETURN n", function (result) {
      console.log("Record matched.");
      var alice = result.records[0].get("n");
      console.log(alice.labels[0]);
      console.log(alice.properties["name"]);
      session.close();
      driver.close();
    });
  });
});
```

### Limitations

Memgraph is currently in early stage, and has a number of limitations we plan
to remove in future versions.

#### Multiple Users & Authorization

Memgraph is currently single-user only. There is no way to control user
privileges. The default user has read and write privileges over the whole
database.

#### Multiple Databases

Currently, a single Memgraph process exposes only one database that is
implicitly used. To use multiple databases, it is necessary to launch multiple
Memgraph processes.

#### Secure Sockets Layer (SSL)

Secure connections are not supported. For this reason each client
driver needs to be configured not to use encryption. Consult driver-specific
guides for details.


#### Node and edge IDs
Unique node and edge IDs are returned by the Bolt protocol to the client. In Memgraph
these IDs are not guaranteed to be persistent during the whole database lifetime. They
should never be used for any client side logic outside a single query/transaction scope.
