## Bolt Drivers

Clients connect to Memgraph using the
[Bolt protocol](https://boltprotocol.org/). Bolt was designed for efficient
communication with graph databases. Memgraph supports
[Version 1](https://boltprotocol.org/v1/) of the protocol. Official Bolt
protocol drivers are provided for multiple programming languages:

  * [Java](https://github.com/neo4j/neo4j-java-driver)
  * [Python](https://github.com/neo4j/neo4j-python-driver)
  * [Javascript](https://github.com/neo4j/neo4j-javascript-driver)
  * [C#](https://github.com/neo4j/neo4j-dotnet-driver)

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

### C# Driver Example

The C# driver is hosted
[on GitHub](https://github.com/neo4j/neo4j-dotnet-driver). The example below
performs the same work as all of the previous examples. Encryption is disabled
by setting `EncryptionLevel.NONE` on the `Config`.

```
using System;
using System.Linq;
using Neo4j.Driver.V1;

public class Basic {
  public static void Main(string[] args) {
    // Initialize the driver.
    var config = Config.DefaultConfig;
    config.EncryptionLevel = EncryptionLevel.None;
    using(var driver = GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None, config))
      using(var session = driver.Session())
      {
        // Run basic queries.
        session.Run("MATCH (n) DETACH DELETE n").Consume();
        session.Run("CREATE (alice:Person {name: \"Alice\", age: 22})").Consume();
        var result = session.Run("MATCH (n) RETURN n").First();
        var alice = (INode) result["n"];
        Console.WriteLine(alice["name"]);
        Console.WriteLine(string.Join(", ", alice.Labels));
        Console.WriteLine(alice["age"]);
      }
    Console.WriteLine("All ok!");
  }
}
```

### Limitations

Memgraph is currently in early stage, and has a number of limitations we plan
to remove in future versions.

#### Multiple Users & Authorization

Memgraph is currently single-user only. There is no way to control user
privileges. The default user has read and write privileges over the whole
database.

#### Secure Sockets Layer (SSL)

Secure connections are not supported. For this reason each client
driver needs to be configured not to use encryption. Consult driver-specific
guides for details.
