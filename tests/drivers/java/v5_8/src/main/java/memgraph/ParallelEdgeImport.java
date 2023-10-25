package memgraph;

import static org.neo4j.driver.Values.parameters;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;

public class ParallelEdgeImport {
  public static int dropDatabase(Driver driver) {
    try (Session session = driver.session()) {
      session.writeTransaction(new TransactionWork<Integer>() {
        @Override
        public Integer execute(Transaction tx) {
          tx.run("MATCH (n) DETACH DELETE n;");
          return 0;
        }
      });
    } catch (ClientException e) {
      System.out.println(e);
    }

    return 0;
  }

  public static int createRoot(Driver driver) {
    try (var session = driver.session()) {
      session.writeTransaction(new TransactionWork<Integer>() {
        @Override
        public Integer execute(Transaction tx) {
          tx.run("MERGE (n:Root) RETURN ID(n);");
          return 0;
        }
      });
    } catch (ClientException e) {
      System.out.println(e);
    }

    return 0;
  }

  public static void parallelEdgeImport(Driver d, int nodeCount) {
    try (var session = driver.session()) {
      session.writeTransaction(new TransactionWork<Integer>() {
        @Override
        public Integer execute(Transaction tx) {
          tx.run("MATCH (root:Root) CREATE (n:Node) CREATE (n)-[:TROUBLING_EDGE]->(root);");
          System.out.println("Transaction got through!");
          return 0;
        }
      });
    } catch (ClientException e) {
      System.out.println(e);
    }
  }

  private static Config config = Config.builder()
                        .withoutEncryption()
                        .withMaxTransactionRetryTime(15, TimeUnit.SECONDS)
                        .build();

  private static Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none(), config);
  private static int nodeCount = 100;

  static class MyRunnable implements Runnable {
    @Override
    public void run() {
        parallelEdgeImport(driver, nodeCount);
    }
  }

  public static void main(String[] args) {
    dropDatabase(driver);
    System.out.println("Database cleared!");

    createRoot(driver);
    System.out.println("Root created!");

    List<Thread> threadList = new ArrayList<>();
    for (int i = 0; i < nodeCount - 1; i++) {
        Runnable myRunnable = new MyRunnable();
        Thread thread = new Thread(myRunnable);
        threadList.add(thread);
        thread.start();
    }

    // Wait for all threads to finish
    for (Thread thread : threadList) {
      try {
          thread.join();
      } catch (InterruptedException e) {
          e.printStackTrace();
      }
    }

    try (var session = driver.session()) {
      var actualNodeCount = session.run("MATCH (n) RETURN COUNT(n) AS cnt;").list().get(0).get("cnt").asInt();
      if (actualNodeCount != nodeCount) {
        System.out.println("Node count doesn't match (" + actualNodeCount + ")");
        System.exit(1);
      }
    }

    System.out.println("All ok!");
    driver.close();
  }
}
