const neo4j = require('neo4j-driver');

function die() {
  session.close();
  driver.close();
  process.exit(1);
}

function Neo4jService(uri) {
  const driver = neo4j.driver(uri, neo4j.auth.basic("", ""));

  async function createGreeting() {
    const session = driver.session({ defaultAccessMode: neo4j.session.WRITE });
    try {
      const result = await session.writeTransaction(tx =>
        tx.run('CREATE (n:Greeting {message: "Hello NodeJs"}) RETURN n.message AS message')
      );
      console.log("Write txn finished");
    } finally {
      await session.close();
    }
  }

  async function close() {
    await driver.close();
  }

  return {
    createGreeting,
    close
  };
}

async function createGreetingsFromUri(uri) {
  const service = Neo4jService(uri);
  await service.createGreeting();
  await service.close();
}

async function main() {
  console.log("Started writing route");
  const uris = [
    'neo4j://localhost:7690',
    'neo4j://localhost:7691',
    'neo4j://localhost:7692'
  ];

  try {
    for (const uri of uris) {
      await createGreetingsFromUri(uri);
    }
  } catch (error) {
    console.error('An error occurred:', error);
    die();
  }
  console.log("Finished writing route");
}

main().catch(error => console.error(error));
