using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Neo4j.Driver;

public class Transactions {
  public static void Main(string[] args) {
    var driver =
        GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None,
                             (builder) => builder.WithEncryptionLevel(EncryptionLevel.None));
    ClearDatabase(driver);
        // Explicit transaction query.
        using (var session = driver.Session()) {
          Console.WriteLine("Checking explicit transaction metadata...");
          var txMetadata = new Dictionary<string, object> {
            { "ver", "transaction" }, { "str", "oho" }, { "num", 456 }
          };
          using (var tx = session.BeginTransaction(txConfig => txConfig.WithMetadata(txMetadata))) {
            tx.Run("MATCH (n) RETURN n LIMIT 1").Consume();
            // Check transaction info from another thread
            Thread show_tx = new Thread(() => ShowTx(ref driver));
            show_tx.Start();
            show_tx.Join();
            // End current transaction
            tx.Commit();
          }
        }
        // Implicit transaction query
        using (var session = driver.Session()) {
          Console.WriteLine("Checking implicit transaction metadata...");
          var txMetadata = new Dictionary<string, object> {
            { "ver", "session" }, { "str", "aha" }, { "num", 123 }
          };
          CheckMD(session.Run("SHOW TRANSACTIONS", txConfig => txConfig.WithMetadata(txMetadata)));
        }

        Console.WriteLine("All ok!");
  }

  private static void ClearDatabase(IDriver driver) {
    using (var session = driver.Session()) session.Run("MATCH (n) DETACH DELETE n").Consume();
  }

  public static void ShowTx(ref IDriver driver) {
    using (var session = driver.Session()) {
      CheckMD(session.Run("SHOW TRANSACTIONS"));
    }
  }

  public static void CheckMD(IResult tx_md) {
    int n = 0;
    try {
      foreach (var res in tx_md) {
        var md = res["metadata"].As<Dictionary<string, object>>();
        if (md.Count != 0) {
          if (md["ver"].As<string>() == "transaction" && md["str"].As<string>() == "oho" &&
              md["num"].As<int>() == 456) {
            n = n + 1;
          } else if (md["ver"].As<string>() == "session" && md["str"].As<string>() == "aha" &&
                     md["num"].As<int>() == 123) {
            n = n + 1;
          }
        }
      }
    } catch {
      n = 0;
    }
    if (n == 0) {
      Console.WriteLine("Metadata error!");
      Environment.Exit(1);
    }
  }
}
