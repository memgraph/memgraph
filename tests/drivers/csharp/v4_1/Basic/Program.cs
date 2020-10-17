using System;
using System.Linq;
using Neo4j.Driver;

public class Basic {
  public static void Main(string[] args) {
    using(var driver = GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None, (ConfigBuilder builder) => builder.WithEncryptionLevel(EncryptionLevel.None)))
      using(var session = driver.Session())
      {
        session.Run("MATCH (n) DETACH DELETE n").Consume();
        session.Run("CREATE (alice:Person {name: \"Alice\", age: 22})").Consume();
        var result = session.Run("MATCH (n) RETURN n").First();
        var alice = (INode) result["n"];
        Console.WriteLine(alice["name"]);
        Console.WriteLine(string.Join(", ", alice.Labels));
        Console.WriteLine(alice["age"]);
      }
    Console.WriteLine("All ok!");
  }
}
