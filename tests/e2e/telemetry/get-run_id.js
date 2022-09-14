const neo4j = require('neo4j-driver');

const runQuery = async (driver, query) => {
  return new Promise((resolve, reject) => {
    const session = driver.session();
    const activeSession = session.run(query);

    activeSession._createSummary = async (metadata) => {
      return metadata;
    };

    activeSession.subscribe({
      onCompleted: async (summary) => {
        await session.close();
        const result = summary;
        resolve(result);
      },
      onError: async (error) => {
        await session.close();
        reject(error);
      },
    });
  })
};

const main = async () => {
  const driver = neo4j.driver('bolt://localhost:7687');

  query = "MATCH (n) RETURN n LIMIT 1;"

  console.log('Query:', query);
  const results = await runQuery(driver, query);
  if (results.hasOwnProperty("run_id")) {
    console.log("run_id:", results["run_id"]);
  } else {
    process.exit(1);
  }
  await driver.close();
};

main(process.argv[2])
  .catch((error) => console.error(error));
