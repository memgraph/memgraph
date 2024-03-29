import static org.neo4j.driver.Values.parameters;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;

public class Metadata {
  public static String createPerson(Transaction tx, String name) {
    Result result =
        tx.run("CREATE (a:Person {name: $name}) RETURN a.name", parameters("name", name));
    return result.single().get(0).asString();
  }

  public static void checkMd(Result result) {
    int n = 0;
    while (result.hasNext()) {
      Record r = result.next();
      Value md = r.get("metadata");
      if (md != null && Objects.equals(md.get("ver").asString(), "transaction")
          && Objects.equals(md.get("str").asString(), "oho")
          && Objects.equals(md.get("num").asInt(), 456)) {
        n = n + 1;
      } else if (md != null && Objects.equals(md.get("ver").asString(), "session")
          && Objects.equals(md.get("str").asString(), "aha")
          && Objects.equals(md.get("num").asInt(), 123)) {
        n = n + 1;
      }
    }
    if (n == 0) {
      System.out.println("Error while reading metadata!");
      System.exit(1);
    }
  }

  public static void main(String[] args) {
    Config config = Config.builder()
                        .withoutEncryption()
                        .withMaxTransactionRetryTime(0, TimeUnit.SECONDS)
                        .build();
    Driver driver =
        GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"), config);

    try (Session session = driver.session()) {
      // Explicit transaction
      System.out.println("Checking explicit transaction metadata...");
      try {
        TransactionConfig tx_config =
            TransactionConfig.builder()
                .withTimeout(Duration.ofSeconds(2))
                .withMetadata(Map.ofEntries(Map.entry("ver", "transaction"),
                    Map.entry("str", "oho"), Map.entry("num", 456)))
                .build();
        Transaction tx = session.beginTransaction(tx_config);
        tx.run("MATCH (n) RETURN n LIMIT 1");
        // Check the metadata from another thread
        try {
          Runnable checkTx = new Runnable() {
            public void run() {
              try (Session s = driver.session()) {
                checkMd(s.run("SHOW TRANSACTIONS"));
              } catch (ClientException e) {
                System.out.println(e);
              }
            }
          };
          Thread thread = new Thread(checkTx);
          thread.start();
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        tx.commit();
      } catch (ClientException e) {
        System.out.println(e);
      }

      // Implicit transaction
      System.out.println("Checking implicit transaction metadata...");
      try {
        TransactionConfig tx_config = TransactionConfig.builder()
                                          .withTimeout(Duration.ofSeconds(2))
                                          .withMetadata(Map.ofEntries(Map.entry("ver", "session"),
                                              Map.entry("str", "aha"), Map.entry("num", 123)))
                                          .build();
        checkMd(session.run("SHOW TRANSACTIONS", tx_config));
      } catch (ClientException e) {
        System.out.println(e);
      }

      System.out.println("All ok!");

    } catch (Exception e) {
      System.out.println(e);
      System.exit(1);
    }

    driver.close();
  }
}
