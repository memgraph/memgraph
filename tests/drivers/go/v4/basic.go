package main

import "github.com/neo4j/neo4j-go-driver/neo4j"
import "os"
import "fmt"

func handle_error(err error) {
  if err != nil {
    fmt.Printf("Error occured: %s", err)
    os.Exit(1)
  }
}

func main() {
  configForNeo4j40 := func(conf *neo4j.Config) { conf.Encrypted = false }

  driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("", "", ""), configForNeo4j40)
  if err != nil {
    fmt.Printf("An error occurred opening conn: %s", err)
    os.Exit(1)
  }

  defer driver.Close()

  sessionConfig := neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}
  session, err := driver.NewSession(sessionConfig)
  if err != nil {
    fmt.Printf("An error occured while creating a session: %s", err)
    os.Exit(1)
  }

  defer session.Close()

  result, err := session.Run("MATCH (n) DETACH DELETE n", map[string]interface{}{})
  handle_error(err)
  _, err = result.Consume()
  handle_error(err)

  result, err = session.Run(`CREATE (alice:Person {name: "Alice", age: 22})`, map[string]interface{}{})
  handle_error(err)
  _, err = result.Consume()
  handle_error(err)

  result, err = session.Run("MATCH (n) RETURN n", map[string]interface{}{})
  handle_error(err)

  if !result.Next() {
    fmt.Printf("Missing result")
  }

  node_record, has_column := result.Record().Get("n")
  if !has_column {
    fmt.Printf("Wrong result returned")
  }
  node_value := node_record.(neo4j.Node)

  fmt.Println(node_value.Props()["name"])
  fmt.Println(node_value.Labels())
  fmt.Println(node_value.Props()["age"])

  fmt.Println("All ok!")
}
