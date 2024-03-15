package memgraph;

import static org.neo4j.driver.Values.parameters;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

public class ReadRouting {
    private Driver driver;

    private void readMessage(String uri) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic("", ""));
        try (Session session = driver.session()) {
            String greeting = session.readTransaction(tx -> {
                var result = tx.run("MATCH (n:Greeting) RETURN n.message AS message");
                System.out.println("Read txn passed!");
                return "OK";
            });
        }
    }

    public static void main(String... args) {
        System.out.println("Started running ReadRoutingTest...");
        ReadRouting greeter = new ReadRouting();
        greeter.readMessage("neo4j://localhost:7690"); // coordinator_1
        greeter.readMessage("neo4j://localhost:7691"); // coordinator_2
        greeter.readMessage("neo4j://localhost:7692"); // coordinator_3
        System.out.println("All good!");
    }
}
