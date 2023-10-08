using System;
using System.Linq;
using Neo4j.Driver;

public class Basic {
  public static void Main(string[] args) {
    using (var driver = GraphDatabase.Driver(
               "bolt://localhost:7687", AuthTokens.None,
               (ConfigBuilder builder) => builder.WithEncryptionLevel(
                   EncryptionLevel.None))) using (var session = driver.Session()) {
      session.Run("MATCH (n) DETACH DELETE n;").Consume();
      Console.WriteLine("Database cleared.");

      session.Run("CREATE (alice:Person {name: \"Alice\", age: 22});").Consume();
      Console.WriteLine("Record created.");

      var node = (INode)session.Run("MATCH (n) RETURN n;").First()["n"];
      Console.WriteLine("Record matched.");

      var label = string.Join("", node.Labels);
      var name = node["name"];
      var age = (long)node["age"];

      if (!label.Equals("Person") || !name.Equals("Alice") || !age.Equals(22)) {
        Console.WriteLine("Data doesn't match!");
        System.Environment.Exit(1);
      }

      Console.WriteLine("Label: " + label);
      Console.WriteLine("name: " + name);
      Console.WriteLine("age: " + age);
    }
    Console.WriteLine("All ok!");
  }
}
