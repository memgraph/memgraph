const neo4j = require('neo4j-driver');

const fixNeo4jMetadata = (metadata) => {
  return { ...metadata, profile: {} };
};

const parseNeo4jSummary = (summary, metadata) => {
  return {
    summary,
    metadata,
  };
}

const runQuery = async (driver, query) => {
  const result = {
    records: [],
    summary: null,
  };

  return new Promise((resolve, reject) => {
    const session = driver.session();
    const activeSession = session.run(query);

    const originalCreateSummary = activeSession._createSummary.bind(activeSession);
    activeSession._createSummary = async (metadata) => {
      const summary = await originalCreateSummary(fixNeo4jMetadata(metadata));
      return parseNeo4jSummary(summary, metadata);
    };

    activeSession.subscribe({
      onCompleted: async (summary) => {
        await session.close();
        result.summary = summary;
        resolve(result);
      },
      onError: async (error) => {
        await session.close();
        reject(error);
      },
    });
  })
};

const main = async (queryPrefix = '') => {
  const driver = neo4j.driver('bolt://localhost:7687');

  query = "MATCH (n) RETURN n LIMIT 1;"

  const newQuery = `${queryPrefix} ${query}`.trim();
  console.log('Query:', newQuery);
  const results = await runQuery(driver, newQuery);
  if (results["summary"]["metadata"].hasOwnProperty("run_id")) {
    console.log("run_id:", results["summary"]["metadata"]["run_id"])
  } else {
    console.log("run_id not found in the summary")
  }
  console.log('');

  await driver.close();
};

main(process.argv[2])
  .catch((error) => console.error(error));
