using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Neo4j.Driver;

public class Transactions {
  public static void Main(string[] args) {
    using (var driver = GraphDatabase.Driver(
               "bolt://localhost:7687", AuthTokens.None,
               (builder) => builder.WithEncryptionLevel(EncryptionLevel.None))) {
      ClearDatabase(driver);
      // Wrong query.
      try {
        using (var session = driver.Session()) using (var tx = session.BeginTransaction()) {
          CreatePerson(tx, "mirko");
          // Incorrectly start CREATE
          tx.Run("CREATE (").Consume();
          CreatePerson(tx, "slavko");
          tx.Commit();
        }
      } catch (ClientException) {
        Console.WriteLine("Rolled back transaction");
      }
      Trace.Assert(CountNodes(driver) == 0, "Expected transaction was rolled back.");
      // Correct query.
      using (var session = driver.Session()) using (var tx = session.BeginTransaction()) {
        CreatePerson(tx, "mirka");
        CreatePerson(tx, "slavka");
        tx.Commit();
      }
      Trace.Assert(CountNodes(driver) == 2, "Expected 2 created nodes.");
      ClearDatabase(driver);
      using (var session = driver.Session()) {
        // Create a lot of nodes so that the next read takes a long time.
        session.Run("UNWIND range(1, 100000) AS i CREATE ()").Consume();
        try {
          Console.WriteLine("Running a long read...");
          session.Run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt").Consume();
        } catch (TransientException) {
          Console.WriteLine("Transaction timed out");
        }
      }
    }
    Console.WriteLine("All ok!");
  }

  private static void CreatePerson(ITransaction tx, string name) {
    var parameters = new Dictionary<string, Object> { { "name", name } };
    var result = tx.Run("CREATE (person:Person {name: $name}) RETURN person", parameters);
    Console.WriteLine("Created: " + ((INode)result.First()["person"])["name"]);
  }

  private static void ClearDatabase(IDriver driver) {
    using (var session = driver.Session()) session.Run("MATCH (n) DETACH DELETE n").Consume();
  }

  private static int CountNodes(IDriver driver) {
    using (var session = driver.Session()) {
      var result = session.Run("MATCH (n) RETURN COUNT(*) AS cnt");
      return Convert.ToInt32(result.First()["cnt"]);
    }
  }
}
