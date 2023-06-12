/**
 * Determines how long could be a query executed
 * from Java driver.
 *
 * Performs binary search until the maximum possible
 * query size has found.
 */
package memgraph;

import static org.neo4j.driver.Values.parameters;

import java.util.*;
import org.neo4j.driver.*;
import org.neo4j.driver.types.*;

public class MaxQueryLength {
  public static void main(String[] args) {
    // init driver
    Config config = Config.builder().withoutEncryption().build();
    Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("", ""), config);
    // init query
    int property_size = 0;
    int min_len = 1;
    int max_len = 100000;
    String query_template = "CREATE (n {name:\"%s\"})";
    int template_size = query_template.length() - 2; // because of %s

    // binary search
    while (true) {
      property_size = (max_len + min_len) / 2;
      try (Session session = driver.session()) {
        String property_value = new String(new char[property_size]).replace('\0', 'a');
        String query = String.format(query_template, property_value);
        session.run(query).consume();
        if (min_len == max_len || property_size + 1 > max_len) {
          break;
        }
        min_len = property_size + 1;
      } catch (Exception e) {
        System.out.println(
            String.format("Query length: %d; Error: %s", property_size + template_size, e));
        max_len = property_size - 1;
      }
    }

    // final result
    System.out.println(String.format("\nThe max length of a query executed from "
            + "Java driver is: %s\n",
        property_size + template_size));

    // cleanup
    driver.close();
  }
}
