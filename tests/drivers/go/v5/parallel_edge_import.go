package main

import (
	"log"
    "fmt"
	"sync"
    "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var nodeCount int = 100

func handle_if_error(err error) {
	if err != nil {
	  log.Fatal("Error occured: %s", err)
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

	_, err = session.WriteTransaction(createRoot)
	handle_if_error(err)
	fmt.Println("Record created.")

	parallelEdgeImport(driver)

	_, err = session.WriteTransaction(testNodeCount)
	handle_if_error(err)
	fmt.Println("All ok!")
}

func clearDatabase(tx neo4j.Transaction) (interface{}, error) {
	result, err := tx.Run(
        "MATCH (n) DETACH DELETE n;",
        map[string]interface{}{})
	handle_if_error(err)
	fmt.Printf("Database cleared!\n")
	return result.Consume()
}

func createRoot(tx neo4j.Transaction) (interface{}, error) {
    result, err := tx.Run(
        `MERGE (root:Root) RETURN root;`,
        map[string]interface{}{})
	handle_if_error(err)
	fmt.Printf("Root created!\n")
    return result.Consume()
}

func worker(driver neo4j.Driver, wg *sync.WaitGroup) {
    defer wg.Done() // Decrement the WaitGroup counter when the goroutine is done

	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err := session.WriteTransaction(edgeImport)
	handle_if_error(err)
    fmt.Printf("Transaction got through!\n")
}

func parallelEdgeImport(driver neo4j.Driver)  {
    var wg sync.WaitGroup

    for i := 1; i <= nodeCount - 1; i++ {
        wg.Add(1)
        go worker(driver, &wg)
    }

    wg.Wait()

    fmt.Println("All workers have finished.")
}

func edgeImport(tx neo4j.Transaction) (interface{}, error) {
    result, err := tx.Run(
        `MATCH (root:Root) CREATE (n:Node) CREATE (n)-[:TROUBLING_EDGE]->(root);`,
        map[string]interface{}{})
	handle_if_error(err)
    return result.Consume()
}

func testNodeCount(tx neo4j.Transaction) (interface{}, error) {
	result, err := tx.Run(
        "MATCH (n) RETURN COUNT(n) AS cnt;",
        map[string]interface{}{})
	handle_if_error(err)

	if !result.Next() {
		log.Fatal("Missing result.")
	}

	count, found := result.Record().Get("cnt")
	if !found {
	  return nil, fmt.Errorf("Wrong result returned.")
	}

    var countInt int
    switch v := count.(type) {
    case int:
        countInt = v
    case int64:
        countInt = int(v)
    case float64:
        countInt = int(v)
    default:
        return nil, fmt.Errorf("Unexpected data type for count: %T", count)
    }

	if countInt != nodeCount {
		log.Fatal("Count does not match! (", count, ")")
	}

    return result.Consume()
}
