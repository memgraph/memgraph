package main

import (
    "fmt"
    "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
    fmt.Println("Started running read_route.go test")
    uri := "neo4j://localhost:7688"
    username := ""
    password := ""

    // Connect to Memgraph
    driver, err := neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    // Use AccessModeRead for read transactions
    session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
    defer session.Close()

    greeting, err := session.ReadTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
        result, err := transaction.Run("MATCH (n:Greeting) RETURN n.message AS message LIMIT 1", nil)
        if err != nil {
            return nil, err
        }

        if result.Next() {
            return result.Record().Values[0], nil
        }

        return nil, result.Err()
    })

    if err != nil {
        panic(err)
    }

    fmt.Println(greeting)
    fmt.Println("Successfully finished running coordinator_route.go test")
}
