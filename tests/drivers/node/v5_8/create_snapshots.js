// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const neo4j = require('neo4j-driver');

// Neo4j connection details
const URI = "bolt://localhost:7687";

class Neo4jOperations {
    constructor(uri) {
        this.driver = neo4j.driver(uri, neo4j.auth.basic("", ""), { maxTransactionRetryTime: 0 });
        console.log("Neo4j driver initialized");
    }

    close() {
        this.driver.close();
        console.log("Neo4j driver closed");
    }

    // Transaction function
    async explicitTx(tx, query) {
        await tx.run(query);
    }

    // Function for implicit transactions (auto-commit)
    async implicitTx(session, query) {
        await session.run(query);
    }
}

async function writeTask(neo4jOps, query = "CREATE ();") {
    const session = neo4jOps.driver.session();
    try {
        console.log("Starting write transaction");
        await session.executeWrite(tx => neo4jOps.explicitTx(tx, query));
        console.log("Write transaction completed");
    } finally {
        await session.close();
    }
}

async function readTask(neo4jOps, query = "MATCH (n) RETURN n LIMIT 1") {
    const session = neo4jOps.driver.session();
    try {
        console.log("Starting read transaction");
        await session.executeRead(tx => neo4jOps.explicitTx(tx, query));
        console.log("Read transaction completed");
    } finally {
        await session.close();
    }
}

async function implicitTask(neo4jOps, query) {
    const session = neo4jOps.driver.session();
    try {
        console.log(`Starting implicit transaction ${query}`);
        await neo4jOps.implicitTx(session, query);
        console.log(`Implicit transaction completed ${query}`);
    } finally {
        await session.close();
    }
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    const neo4jOps = new Neo4jOperations(URI);

    try {
        // Setup (need some data so snapshot takes time)
        const setupSession = neo4jOps.driver.session();
        await setupSession.run("MATCH (n) DETACH DELETE n");
        console.log("Database cleared.");
        await setupSession.run("FREE MEMORY");
        console.log("Memory cleared.");
        await setupSession.run("STORAGE MODE IN_MEMORY_ANALYTICAL");
        console.log("Analytical mode.");
        await setupSession.close();

        // Setup (needs some data for snapshot)
        await writeTask(neo4jOps, "USING PERIODIC COMMIT 10 UNWIND RANGE (1,5000000) as i create (:l)-[:e]->();");

        let failed = 0;
        // Check that a read_only (snapshot) tx allows reads but not writes
        const promises = [];

        promises.push(implicitTask(neo4jOps, "CREATE SNAPSHOT"));
        await sleep(100);
        promises.push(readTask(neo4jOps));
        await sleep(100);
        promises.push(implicitTask(neo4jOps, "MATCH(n) RETURN n LIMIT 1"));
        await sleep(100);

        try {
            await implicitTask(neo4jOps, "CREATE ()");
        } catch (e) {
            console.log(`Write implicit transaction failed as expected: ${e}`);
            failed++;
        }

        try {
            await writeTask(neo4jOps, "create (:l)-[:e]->();");
        } catch (e) {
            console.log(`Write explicit transaction failed as expected: ${e}`);
            failed++;
        }

        await Promise.all(promises);

        if (failed !== 2) {
            throw new Error("Write transaction should not be allowed in read-only mode");
        }

        console.log("Read-only test OK!");

    } catch (e) {
        console.log(`Error occurred: ${e}`);
        process.exit(1);
    } finally {
        const cleanupSession = neo4jOps.driver.session();
        await cleanupSession.run("MATCH (n) DETACH DELETE n");
        console.log("Database cleared.");
        await cleanupSession.run("FREE MEMORY");
        console.log("Memory cleared.");
        await cleanupSession.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL");
        console.log("Instance reset.");
        await cleanupSession.close();
        neo4jOps.close();
    }
}

main().catch(error => {
    console.error(error);
    process.exit(1);
});
