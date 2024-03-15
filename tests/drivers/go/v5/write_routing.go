package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func create_message(uri string) {
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
}

// Test checks that you can use bolt+routing for connecting to main and coordinators for writing.
func main() {
	fmt.Println("Started running main_route.go test")
	create_message("neo4j://localhost:7687") // instance_1
	create_message("neo4j://localhost:7690") // coordinator_1
	create_message("neo4j://localhost:7691") // coordinator_2
	create_message("neo4j://localhost:7692") // coordinator_3
	fmt.Println("Successfully finished running main_route.go test")
}
