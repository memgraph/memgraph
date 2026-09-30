package memgraph;

import static org.neo4j.driver.Values.parameters;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;

public class WriteRouting {
    private Driver driver;

    private void createMessage(String uri) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic("", ""));
        try (Session session = driver.session()) {
            String greeting = session.writeTransaction(tx -> {
                var result = tx.run("CREATE (n:Greeting) SET n.message = $message RETURN n.message",
                                    parameters("message", "Hello, World!"));
                if (result.hasNext()) {
                    return result.single().get(0).asString();
                }
                throw new RuntimeException("No result found.");
            });
            System.out.println(greeting);
        }
    }

    public static void main(String... args) {
        System.out.println("Started running WriteRoutingTest...");
        WriteRouting greeter = new WriteRouting();
        greeter.createMessage("neo4j://localhost:7690"); // coordinator_1
        greeter.createMessage("neo4j://localhost:7691"); // coordinator_2
        greeter.createMessage("neo4j://localhost:7692"); // coordinator_3
        System.out.println("All good!");
    }
}
