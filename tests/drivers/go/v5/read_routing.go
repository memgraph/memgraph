package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func read_messages(uri string) {
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

}

// Test checks that you can use bolt+routing for connecting to main and coordinators for reading.
func main() {
	fmt.Println("Started running read_route.go test")
	read_messages("neo4j://localhost:7690") // coordinator_1
	read_messages("neo4j://localhost:7691") // coordinator_2
	read_messages("neo4j://localhost:7692") // coordinator_3
	fmt.Println("Successfully finished running read_route.go test")
}
