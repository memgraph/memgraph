import static org.neo4j.driver.v1.Values.parameters;

import java.util.*;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.*;

public class Basic {
  public static void main(String[] args) {
    Config config = Config.build().withoutEncryption().toConfig();
    Driver driver =
        GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"), config);

    try (Session session = driver.session()) {
      StatementResult rs1 = session.run("MATCH (n) DETACH DELETE n");
      System.out.println("Database cleared.");

      StatementResult rs2 = session.run("CREATE (alice: Person {name: 'Alice', age: 22})");
      System.out.println("Record created.");

      StatementResult rs3 = session.run("MATCH (n) RETURN n");
      System.out.println("Record matched.");

      List<org.neo4j.driver.v1.Record> records = rs3.list();
      org.neo4j.driver.v1.Record record = records.get(0);
      Node node = record.get("n").asNode();
      if (!node.get("name").asString().equals("Alice") || node.get("age").asInt() != 22) {
        System.out.println("Data doesn't match!");
        System.exit(1);
      }

      System.out.println("All ok!");
    } catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }

    driver.close();
  }
}
