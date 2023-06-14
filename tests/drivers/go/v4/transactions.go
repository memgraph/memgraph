package main

import "github.com/neo4j/neo4j-go-driver/neo4j"
import "log"
import "fmt"

func handle_error(err error) {
  log.Fatal("Error occurred: %s", err)
}

func create_person(tx neo4j.Transaction, name string) interface{} {
    result, err := tx.Run("CREATE (a:Person {name: $name}) RETURN a", map[string]interface{}{
      "name": name,
    })
    if err != nil {
      handle_error(err)
    }

    if !result.Next() {
      log.Fatal("Missing results!");
    }
    node, has_column := result.Record().Get("a")
    if !has_column {
      log.Fatal("Wrong results")
    }
    node_value := node.(neo4j.Node)
    return node_value.Props()["name"]
}

func main() {
  configForNeo4j40 := func(conf *neo4j.Config) { conf.Encrypted = false }

  driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("", "", ""), configForNeo4j40)
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

  session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
    fmt.Println(create_person(tx, "mirko"))
    result, err := tx.Run("CREATE (", map[string]interface{}{})
    if err != nil {
      handle_error(err)
    }
    _, err = result.Consume()
    if err == nil {
      log.Fatal("The query should have failed")
    } else {
      fmt.Println("The query failed as expected")
    }

    return nil, nil
  })

  session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
    fmt.Println(create_person(tx, "mirko"))
    fmt.Println(create_person(tx, "slavko"))
    return nil, nil
  })

  result, err := session.Run("UNWIND range(1, 100000) AS x CREATE ()", map[string]interface{}{})
  if err != nil {
    handle_error(err)
  }
  _, err = result.Consume()
  if err != nil {
    handle_error(err)
  }
  session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
    result, err := tx.Run("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*) AS cnt", map[string]interface{}{})
    if err != nil {
      handle_error(err)
    }

    _, err = result.Consume()
    if err == nil {
      log.Fatal("The query should have timed out")
    } else {
      fmt.Println("The query timed out as expected")
    }
    return nil, nil
  })

  fmt.Println("All ok!")
}
