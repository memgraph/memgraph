using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Neo4j.Driver;

public class ParallelEdgeImport {
  public static async Task Main(string[] args) {
    var nodeCount = 100;
    using (var driver = GraphDatabase.Driver(
               "bolt://localhost:7687", AuthTokens.None,
               (builder) => builder.WithEncryptionLevel(EncryptionLevel.None))) {
      ClearDatabase(driver);

      // Create root
      using (var session = driver.Session()) {
        session.ExecuteWrite(tx => {
          var result = tx.Run("MERGE (root:Root) RETURN ID(root);");
          return result.Single()[0].As<int>();
        });
      }

      Trace.Assert(CountNodes(driver) == 1, "Node count after root creation is not correct!.");

      // Importing edges asynchronously
      var tasks = new List<Task>();
      for (int i = 0; i < nodeCount - 1; i++) {
        tasks.Add(ImportEdgeAsync(driver));
      }

      await Task.WhenAll(tasks);

      Trace.Assert(CountNodes(driver) == nodeCount);
    }

    Console.WriteLine("All ok!");
  }

  private static void ClearDatabase(IDriver driver) {
    using (var session = driver.Session()) session.Run("MATCH (n) DETACH DELETE n").Consume();
  }

  private static async Task ImportEdgeAsync(IDriver driver) {
    using (var session = driver.AsyncSession()) {
      await session.WriteTransactionAsync(async tx => {
        var reader = await tx.RunAsync(
            "MATCH (root:Root) CREATE (n:Node) CREATE (n)-[:TROUBLING_EDGE]->(root) RETURN ID(root)");
        await reader.FetchAsync();
        Console.WriteLine("Transaction got through!");
        return reader.Current[0];
      });
    }
  }

  private static int CountNodes(IDriver driver) {
    using (var session = driver.Session()) {
      var result = session.Run("MATCH (n) RETURN COUNT(*) AS cnt");
      return Convert.ToInt32(result.First()["cnt"]);
    }
  }
}
