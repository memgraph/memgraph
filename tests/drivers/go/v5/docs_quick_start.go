package main

import (
	"log"
    "fmt"
    "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func handle_if_error(err error) {
	if err != nil {
	  log.Fatal("Error occurred: %s", err)
	}
  }

func main() {
    dbUri := "bolt://localhost:7687"
    driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("", "", ""))
    if err != nil {
        log.Fatal("An error occurred opening conn: %s", err)
    }
    defer driver.Close()

	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err = session.WriteTransaction(clearDatabase)
	handle_if_error(err)
	fmt.Println("Database cleared.")

	_, err = session.WriteTransaction(createItemFn)
	handle_if_error(err)
	fmt.Println("Record created.")

	_,err = session.WriteTransaction(testAll)
	handle_if_error(err)
	fmt.Println("All ok!")
}

func clearDatabase(tx neo4j.Transaction) (interface{}, error) {
	result, err := tx.Run(
        "MATCH (n) DETACH DELETE n;",
        map[string]interface{}{})
	handle_if_error(err)
	return result.Consume()
}

func createItemFn(tx neo4j.Transaction) (interface{}, error) {
    result, err := tx.Run(
        `CREATE (alice:Person {name: "Alice", age: 22});`,
        map[string]interface{}{})
	handle_if_error(err)
    return result.Consume()
}

func testAll(tx neo4j.Transaction) (interface{}, error) {
	result, err := tx.Run(
        "MATCH (n) RETURN n;",
        map[string]interface{}{})
	handle_if_error(err)

	if !result.Next() {
		log.Fatal("Missing result.")
	}

	node_record, found := result.Record().Get("n")
	if !found {
	  return nil, fmt.Errorf("Wrong result returned.")
	}

	node_value := node_record.(neo4j.Node)
	fmt.Println("Record matched.")

	label := node_value.Labels[0]
	name, err := neo4j.GetProperty[string](node_value, "name")
	handle_if_error(err)
	age, err := neo4j.GetProperty[int64](node_value, "age")
	handle_if_error(err)
  
	if label != "Person" && name != "Alice" && age != 22 {
	  return nil, fmt.Errorf("Data doesn't match.")
	}
  
	fmt.Println("Label", label)
	fmt.Println("name", name)
	fmt.Println("age", age)

    return result.Consume()
}

