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

public class Transactions {
  public static String createPerson(Transaction tx, String name) {
    Result result =
        tx.run("CREATE (a:Person {name: $name}) RETURN a.name", parameters("name", name));
    return result.single().get(0).asString();
  }

  public static void main(String[] args) {
    Config config = Config.builder()
                        .withoutEncryption()
                        .withMaxTransactionRetryTime(0, TimeUnit.SECONDS)
                        .build();
    Driver driver =
        GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"), config);

    try (Session session = driver.session()) {
      try {
        session.writeTransaction(new TransactionWork<String>() {
          @Override
          public String execute(Transaction tx) {
            createPerson(tx, "mirko");
            Result result = tx.run("CREATE (");
            return result.single().get(0).asString();
          }
        });
      } catch (ClientException e) {
        System.out.println(e);
      }

      session.writeTransaction(new TransactionWork<String>() {
        @Override
        public String execute(Transaction tx) {
          System.out.println(createPerson(tx, "mirko"));
          System.out.println(createPerson(tx, "slavko"));
          return "Done";
        }
      });

      System.out.println("All ok!");

      boolean timed_out = false;
      try {
        session.writeTransaction(new TransactionWork<String>() {
          @Override
          public String execute(Transaction tx) {
            Result result = tx.run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt");
            return result.single().get(0).asString();
          }
        });
      } catch (TransientException e) {
        timed_out = true;
      }

      if (timed_out) {
        System.out.println("The query timed out as was expected.");
      } else {
        throw new Exception("The query should have timed out, but it didn't!");
      }
    } catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }

    driver.close();
  }
}
