## Quick Start

This chapter outlines several ways to connect and execute openCypher queries
against Memgraph using available Bolt clients and drivers.

Before the start, it is important to mention a couple of current limitations.
Memgraph doesn't yet support multi-command transactions.  In other words,
explicit start and termination of transactions isn't yet supported. A
transaction is created and committed implicitly before and after each `run`
method.

SSL is also not supported yet, so a driver has to be configured without
encryption.  This is usually accomplished by modifying the configuration
parameters before using them to construct the driver. There should be a
parameter controlling the encryption and it should be set to a disabled state.
Furthermore, the authorization isn't supported yet.  Username and password
parameters are ignored during the authorization process. In other words, any
combination of username and password will work, preferably they should be empty.

Memgraph is 100% Bolt compliant.  That means every Bolt compliant driver should
work. The following clients and drivers are tested `libneo4j-client` (C),
`neo4j-driver` (Python), `neo4j-driver` (Javascript), `Neo4j Driver` (Java).

### neo4j-client Example

Please take a look [here](https://neo4j-client.net) for the installation details
and complete documentation.

#### Query execution

You can execute a single query by piping the query string to the client
```
echo "MATCH (n) RETURN n;" | neo4j-client --insecure -u "" -p "" localhost 7687
```
or start the client and then make the connection. The most important thing is
not to forget `--insecure` parameter to disable SSL.
```
neo4j-client --insecure
# to establish a connection the connect command has to be excuted as follows
:connect localhost 7687
```
Each query written in the `neo4j-client` console has to be finished with `;`
e.g.
```
MATCH ()-[r]->() RETURN count(r);
```

### Python Driver Example

The details about Python driver can be found [on
GitHub](https://github.com/neo4j/neo4j-python-driver).  Below is a very basic
example how to execute queries against Memgraph.

Similar to all other drivers, driver and session have to be initialized (at the
end of execution they also have to be closed).  To execute a query, `run` method
has to be called. `consume` is here just to block the execution until the query
is actually executed.  At the end of the code snippet shown below, the `print`
function is used to display properties and labels of the obtained data.

```
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=basic_auth("", ""),
                              encrypted=False)
session = driver.session()

session.run('MATCH (n) DETACH DELETE n').consume()
session.run('CREATE (alice:Person {name: "Alice", age: 22})').consume()

returned_result_set = session.run('MATCH (n) RETURN n').data()
returned_result = returned_result_set.pop()
alice = returned_result["n"]

print(alice['name'])
print(alice.labels)
print(alice['age'])

session.close()
driver.close()
```

### Java Driver Example

The details about Java driver can be found [on
GitHub](https://github.com/neo4j/neo4j-java-driver).

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
        // Init driver.
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

The details about Javascript driver can be found [on
GitHub] (https://github.com/neo4j/neo4j-javascript-driver).

The Javascript example below is equivalent to Python and Java examples. SSL can
be disabled by passing `{encrypted: 'ENCRYPTION_OFF'}` during the driver
construction.

Here is an example related to `Node.js`. Memgraph doesn't have integrated
support for `Web Socket` which is required during the execution in any web
browser. If you want to run `openCypher` queries from a web browser,
[websockify](https://github.com/novnc/websockify) has to be up and running.
Requests from web browsers are wrapped into `Web Socket` messages, and a proxy
is needed to handle the overhead. The proxy has to be configured to point out to
Memgraph's Bolt port and web browser driver has to send requests to the proxy
port.

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
