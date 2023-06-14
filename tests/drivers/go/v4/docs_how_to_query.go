package main

import "github.com/neo4j/neo4j-go-driver/neo4j"
import "fmt"
import "log"

func handle_if_error(err error) {
  if err != nil {
    log.Fatal("Error occurred: %s", err)
  }
}

func main() {
  configForNeo4j40 := func(conf *neo4j.Config) { conf.Encrypted = false }
  driver, err := neo4j.NewDriver("bolt://localhost:7687",
                                 neo4j.BasicAuth("", "", ""),
                                 configForNeo4j40)
  if err != nil {
    log.Fatal("An error occurred opening conn: %s", err)
  }
  defer driver.Close()

  sessionConfig := neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}
  session, err := driver.NewSession(sessionConfig)
  if err != nil {
    log.Fatal("An error occurred while creating a session: %s", err)
  }
  defer session.Close()

  result, err := session.Run("MATCH (n) DETACH DELETE n;",
                             map[string]interface{}{})
  handle_if_error(err)
  _, err = result.Consume()
  handle_if_error(err)
  fmt.Println("Database cleared.")

  result, err = session.Run(`CREATE (alice:Person {name: "Alice", age: 22});`,
                            map[string]interface{}{})
  handle_if_error(err)
  _, err = result.Consume()
  handle_if_error(err)
  fmt.Println("Record created.")

  result, err = session.Run("MATCH (n) RETURN n;", map[string]interface{}{})
  handle_if_error(err)
  if !result.Next() {
    log.Fatal("Missing result.")
  }
  node_record, has_column := result.Record().Get("n")
  if !has_column {
    log.Fatal("Wrong result returned.")
  }
  node_value := node_record.(neo4j.Node)
  fmt.Println("Record matched.")

  label := node_value.Labels()[0]
  name := node_value.Props()["name"]
  age := node_value.Props()["age"]

  if label != "Person" && name != "Alice" && age != 22 {
    log.Fatal("Data doesn't match.")
  }

  fmt.Println("Label", label)
  fmt.Println("name", name)
  fmt.Println("age", age)

  fmt.Println("All ok!")
}
