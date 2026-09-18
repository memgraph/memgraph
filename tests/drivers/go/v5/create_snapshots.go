package main

import (
	"context"
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func handle_if_error(err error) {
    if err != nil {
        log.Fatal("Error occurred: %s", err)
    }
}

type Neo4jOperations struct {
    driver neo4j.Driver
    driver_w_ctx neo4j.DriverWithContext
}

func NewNeo4jOperations(uri string) (*Neo4jOperations, error) {
    driver, err := neo4j.NewDriver(uri, neo4j.BasicAuth("", "", ""), func(config *neo4j.Config) {
        config.MaxTransactionRetryTime = 0 * time.Second
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
    }
    driver_w_ctx, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth("", "", ""), func(config *neo4j.Config) {
        config.MaxTransactionRetryTime = 0 * time.Second
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
    }
    fmt.Println("Neo4j driver initialized")
    return &Neo4jOperations{driver: driver, driver_w_ctx: driver_w_ctx}, nil
}

func (n *Neo4jOperations) Close() {
	ctx := context.Background()
    n.driver.Close()
	n.driver_w_ctx.Close(ctx)
    fmt.Println("Neo4j driver closed")
}

func (n *Neo4jOperations) explicit_tx(tx neo4j.Transaction, query string) (interface{}, error) {
    result, err := tx.Run(query, map[string]interface{}{})
    if err != nil {
        return nil, fmt.Errorf("explicit transaction failed: %w", err)
    }
    return result.Consume()
}

func (n *Neo4jOperations) implicit_tx(session neo4j.SessionWithContext, query string) (interface{}, error) {
	ctx := context.Background()
    result, err := session.Run(ctx, query, map[string]interface{}{})
    if err != nil {
        return nil, fmt.Errorf("implicit transaction failed: %w", err)
    }
    return result.Consume(ctx)
}

func write_task(neo4j_ops *Neo4jOperations, query string) error {
    session := neo4j_ops.driver.NewSession(neo4j.SessionConfig{})
    defer session.Close()

    fmt.Println("Starting write transaction")
    _, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
        return neo4j_ops.explicit_tx(tx, query)
    })
    if err != nil {
        return fmt.Errorf("write transaction failed: %w", err)
    }

    fmt.Println("Write transaction completed")
    return nil
}

func read_task(neo4j_ops *Neo4jOperations, query string) error {
    session := neo4j_ops.driver.NewSession(neo4j.SessionConfig{})
    defer session.Close()

    fmt.Println("Starting read transaction")
    _, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
        return neo4j_ops.explicit_tx(tx, query)
    })
    if err != nil {
        return fmt.Errorf("read transaction failed: %w", err)
    }

    fmt.Println("Read transaction completed")
    return nil
}

func implicit_task(neo4j_ops *Neo4jOperations, query string) error {
	ctx := context.Background()
    session := neo4j_ops.driver_w_ctx.NewSession(ctx, neo4j.SessionConfig{})
    defer session.Close(ctx)

    fmt.Printf("Starting implicit transaction %s\n", query)
    _, err := neo4j_ops.implicit_tx(session, query)
    if err != nil {
        return fmt.Errorf("implicit transaction failed: %w", err)
    }

    fmt.Printf("Implicit transaction completed %s\n", query)
    return nil
}

func clear_database(neo4j_ops *Neo4jOperations) error {
	ctx := context.Background()
    session := neo4j_ops.driver_w_ctx.NewSession(ctx, neo4j.SessionConfig{})
    defer session.Close(ctx)

    result, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Database cleared.")

    result, err = session.Run(ctx, "FREE MEMORY", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Memory cleared.")

    result, err = session.Run(ctx, "STORAGE MODE IN_MEMORY_ANALYTICAL", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Analytical mode.")
    return nil
}

func reset_database(neo4j_ops *Neo4jOperations) error {
	ctx := context.Background()
    session := neo4j_ops.driver_w_ctx.NewSession(ctx, neo4j.SessionConfig{})
    defer session.Close(ctx)

    result, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Database cleared.")

    result, err = session.Run(ctx, "FREE MEMORY", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Memory cleared.")

    result, err = session.Run(ctx, "STORAGE MODE IN_MEMORY_TRANSACTIONAL", nil)
    if err != nil {
        return err
    }
	result.Consume(ctx)
    fmt.Println("Instance reset.")
    return nil
}

func main() {
    URI := "bolt://localhost:7687"
    neo4j_ops, err := NewNeo4jOperations(URI)
    if err != nil {
        log.Fatalf("Failed to initialize Neo4j operations: %s", err)
    }
    defer neo4j_ops.Close()

    var wg sync.WaitGroup
    failed := 0

    if err := clear_database(neo4j_ops); err != nil {
        log.Fatalf("Failed to clear database: %s", err)
    }

    // Split query into smaller ones, times out in DEBUG
    for i := 0; i < 50; i++ {
        if err := write_task(neo4j_ops, "USING PERIODIC COMMIT 10 UNWIND RANGE (1,100000) as i create (:l)-[:e]->();"); err != nil {
            log.Fatalf("Failed to write initial data: %s", err)
        }
    }

    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := implicit_task(neo4j_ops, "CREATE SNAPSHOT"); err != nil {
            log.Fatalf("Failed to create snapshot: %s", err)
        }
    }()

    time.Sleep(100 * time.Millisecond)

    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := read_task(neo4j_ops, "MATCH (n) RETURN n LIMIT 1"); err != nil {
            log.Fatalf("Read task failed: %s", err)
        }
    }()

    time.Sleep(100 * time.Millisecond)

    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := implicit_task(neo4j_ops, "MATCH (n) RETURN n LIMIT 1"); err != nil {
            log.Fatalf("Implicit read task failed: %s", err)
        }
    }()

    time.Sleep(100 * time.Millisecond)

    wg.Add(1)
    go func() {
        defer wg.Done()
        err := implicit_task(neo4j_ops, "CREATE ()")
        if err == nil {
            log.Fatalf("Write implicit transaction should have failed but did not")
        } else {
            fmt.Printf("Write implicit transaction failed as expected: %s\n", err)
            failed++
        }
    }()

    time.Sleep(100 * time.Millisecond)

    wg.Add(1)
    go func() {
        defer wg.Done()
        err := write_task(neo4j_ops, "create (:l)-[:e]->();")
        if err == nil {
            log.Fatalf("Write explicit transaction should have failed but did not")
        } else {
            fmt.Printf("Write explicit transaction failed as expected: %s\n", err)
            failed++
        }
    }()

    wg.Wait()

    if failed != 2 {
        log.Fatalf("Write transaction should not be allowed in read-only mode, failed: %d", failed)
    }

    fmt.Println("Read-only test OK!")

    if err := reset_database(neo4j_ops); err != nil {
        log.Fatalf("Failed to reset database: %s", err)
    }
}
