package main

import "github.com/neo4j/neo4j-go-driver/neo4j"
import "log"
import "fmt"

func handle_error(err error) {
  log.Fatal("Error occured: %s", err)
}

func check_md(result neo4j.Result, err error) {
  if err != nil {
    handle_error(err)
  }
  n := 0
  for result.Next() {
    md, ok := result.Record().Get("metadata")
    if !ok {
      log.Fatal("Failed to read metadata!")
    }
    md_map, ok := md.(map[string]interface{})
    if ok && md_map["ver"].(string) == "session" && md_map["str"].(string) == "aha" && md_map["num"].(int64) == 123{
      n++
    }
    else if ok && md_map["ver"].(string) == "transaction" && md_map["str"].(string) == "oho" && md_map["num"].(int64) == 456{
      n++
    }
  }
  if n == 0 {
    log.Fatal("Wrong metadata values!")
  }
  _, err = result.Consume()
  if err != nil {
    handle_error(err)
  }
}

func check_tx(driver neo4j.Driver) {
  sessionConfig := neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}
  session, err := driver.NewSession(sessionConfig)
  if err != nil {
    log.Fatal("An error occured while creating a session: %s", err)
  }

  defer session.Close()

  result, err := session.Run("SHOW TRANSACTIONS", nil)
  check_md(result, err)
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
    log.Fatal("An error occured while creating a session: %s", err)
  }

  defer session.Close()

  // Implicit transaction
  result, err := session.Run("SHOW TRANSACTIONS", nil, neo4j.WithTxMetadata(map[string]interface{}{"ver":"session", "str":"aha", "num":123}))
  check_md(result, err)

  // Explicit transaction
  tx, err := session.BeginTransaction(neo4j.WithTxMetadata(map[string]interface{}{"ver":"transaction", "str":"oho", "num":456}))
  if err != nil {
    handle_error(err)
  }
  tx.Run("MATCH (n) RETURN n LIMIT 1", map[string]interface{}{})
  go check_tx(driver)
  tx.Commit()

  fmt.Println("All ok!")
}
