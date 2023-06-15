package memgraph;

import java.util.*;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Config;

import static org.neo4j.driver.Values.parameters;

public class DocsHowToQuery {
  public static void main(String[] args) {
    var config = Config.builder().withoutEncryption().build();
    var driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("", ""), config);

    try (var session = driver.session()) {
      session.run("MATCH (n) DETACH DELETE n;");
      System.out.println("Database cleared.");

      session.run("CREATE (alice:Person {name: 'Alice', age: 22});");
      System.out.println("Record created.");

      var node = session.run("MATCH (n) RETURN n;").list().get(0).get("n").asNode();
      System.out.println("Record matched.");

      var label = node.labels().iterator().next();
      var name = node.get("name").asString();
      var age = node.get("age").asInt();

      if (!label.equals("Person") || !name.equals("Alice") || age != 22) {
        System.out.println("Data doesn't match!");
        System.exit(1);
      }

      System.out.println("Label: " + label);
      System.out.println("name: " + name);
      System.out.println("age: " + age);

      System.out.println("All ok!");
    } catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }

    driver.close();
  }
}
