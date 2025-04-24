package memgraph;

import static org.neo4j.driver.SessionConfig.builder;

import java.util.concurrent.*;
import org.neo4j.driver.*;

public class CreateSnapshots {
  private static final String URI = "bolt://localhost:7687";
  private final Driver driver;

  public CreateSnapshots(String uri) {
    Config config = Config.builder()
                        .withoutEncryption()
                        .withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS)
                        .build();
    this.driver = GraphDatabase.driver(uri, AuthTokens.none(), config);
    System.out.println("Neo4j driver initialized");
  }

  public void close() {
    driver.close();
    System.out.println("Neo4j driver closed");
  }

  // Transaction functions
  private void explicitTx(TransactionContext tx, String query) {
    tx.run(query).consume();
  }

  private void implicitTx(Session session, String query) {
    session.run(query).consume();
  }

  private void writeTask(String query) {
    try (Session session = driver.session()) {
      System.out.println("Starting write transaction");
      session.executeWrite(tx -> {
        explicitTx(tx, query);
        return null;
      });
      System.out.println("Write transaction completed");
    }
  }

  private void readTask(String query) {
    try (Session session = driver.session()) {
      System.out.println("Starting read transaction");
      session.executeRead(tx -> {
        explicitTx(tx, query);
        return null;
      });
      System.out.println("Read transaction completed");
    }
  }

  private void implicitTask(String query) {
    try (Session session = driver.session()) {
      System.out.println("Starting implicit transaction " + query);
      implicitTx(session, query);
      System.out.println("Implicit transaction completed " + query);
    }
  }

  public static void main(String[] args) {
    CreateSnapshots ops = new CreateSnapshots(URI);
    ExecutorService executor = Executors.newFixedThreadPool(4);

    try {
      // Setup
      try (Session session = ops.driver.session()) {
        session.run("MATCH (n) DETACH DELETE n").consume();
        System.out.println("Database cleared.");
        session.run("FREE MEMORY").consume();
        System.out.println("Memory cleared.");
        session.run("STORAGE MODE IN_MEMORY_ANALYTICAL").consume();
        System.out.println("Analytical mode.");
      }

      // Setup (needs some data for snapshot)
      ops.writeTask("UNWIND RANGE(1,5000000) as i CREATE (:l)-[:e]->();");

      int failed = 0;
      // Check that a read_only (snapshot) tx allows reads but not writes
      Future<?> snapshotFuture = executor.submit(() -> ops.implicitTask("CREATE SNAPSHOT"));

      Thread.sleep(100); // Wait 100ms
      Future<?> readFuture = executor.submit(() -> ops.readTask("MATCH (n) RETURN n LIMIT 1"));

      Thread.sleep(100);
      Future<?> implReadFuture =
          executor.submit(() -> ops.implicitTask("MATCH(n) RETURN n LIMIT 1"));

      Thread.sleep(100);
      try {
        ops.implicitTask("CREATE ()");
      } catch (Exception e) {
        System.out.println("Write implicit transaction failed as expected: " + e);
        failed++;
      }

      try {
        ops.writeTask("UNWIND RANGE(1,5000000) as i CREATE (:l)-[:e]->();");
      } catch (Exception e) {
        System.out.println("Write explicit transaction failed as expected: " + e);
        failed++;
      }

      // Wait for all tasks to complete
      snapshotFuture.get();
      readFuture.get();
      implReadFuture.get();

      if (failed != 2) {
        throw new RuntimeException("Write transaction should not be allowed in read-only mode");
      }

      System.out.println("Read-only test OK!");

    } catch (Exception e) {
      System.out.println("Error occurred: " + e);
      System.exit(1);
    } finally {
      try (Session session = ops.driver.session()) {
        session.run("MATCH (n) DETACH DELETE n").consume();
        System.out.println("Database cleared.");
        session.run("FREE MEMORY").consume();
        System.out.println("Memory cleared.");
        session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL").consume();
        System.out.println("Instance reset.");
      }
      ops.close();
      executor.shutdown();
    }
  }
}
