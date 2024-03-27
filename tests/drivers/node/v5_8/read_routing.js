const neo4j = require('neo4j-driver');

function die() {
  session.close();
  driver.close();
  process.exit(1);
}

function Neo4jService(uri) {
  const driver = neo4j.driver(uri, neo4j.auth.basic("", ""));

  async function readGreeting() {
    const session = driver.session({ defaultAccessMode: neo4j.session.READ });
    try {
      const result = await session.readTransaction(tx =>
        tx.run('MATCH (n:Greeting) RETURN n.message AS message')
      );
      console.log("Read txn finished");
    } finally {
      await session.close();
    }
  }

  async function close() {
    await driver.close();
  }

  return {
    readGreeting,
    close
  };
}

async function readGreetingsFromUri(uri) {
  const service = Neo4jService(uri);
  await service.readGreeting();
  await service.close();
}

async function main() {
  console.log("Started reading route");
  const uris = [
    'neo4j://localhost:7690',
    'neo4j://localhost:7691',
    'neo4j://localhost:7692'
  ];

  try {
    for (const uri of uris) {
      await readGreetingsFromUri(uri);
    }
  } catch (error) {
    console.error('An error occurred:', error);
    die();
  }
  console.log("Finished reading route");
}

main().catch(error => console.error(error));
