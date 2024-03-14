package main

import (
    "fmt"
    "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Test checks that you can use bolt+routing for connecting to coordinator but getting routed to main.
func main() {
    fmt.Println("Started running coordinator_route.go test")
    uri := "neo4j://localhost:7690"
    username := ""
    password := ""

    // Connect to Memgraph
    driver, err := neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
    defer session.Close()

    greeting, err := session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
        result, err := transaction.Run("CREATE (n:Greeting) SET n.message = $message RETURN n.message", map[string]interface{}{
            "message": "Hello, World!",
        })
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
