const _ = require('lodash');
const neo4j = require('neo4j-driver');

const GRAPH_TYPES = ['Node', 'Relationship', 'UnboundRelationship', 'Path', 'PathSegment'];

function parseField(field) {
  if (field === undefined || field === null) {
    return null;
  }

  if (_.isArray(field)) {
    return field.map((f) => parseField(f));
  }

  if (neo4j.isInt(field)) {
    return field.toNumber();
  }

  if (_.isObject(field)) {
    const newObj = {};
    Object.keys(field).forEach((key) => {
      if (_.has(field, key)) {
        newObj[key] = parseField(field[key]);
      }
    });

    const valueName = field.constructor.name;
    if (GRAPH_TYPES.includes(valueName)) {
      newObj.class = valueName;
    }

    return newObj;
  }

  return field;
}

// keys: ['a'], _fields: [5, { ... }, [], ...]
function parseRecord(record) {
  const newRecord = {};
  record.keys.forEach((key, index) => {
    newRecord[key] = parseField(record._fields[index]);
  });
  return newRecord;
}

const fixNeo4jMetadata = (metadata) => {
  return { ...metadata, profile: {} };
};

const parseNeo4jSummary = (summary, metadata) => {
  return {
    summary,
    metadata,
  //   query: {
  //     text: summary.query.text,
  //     parameters: parseField(summary.query.parameters),
  //   },
  //   queryType: summary.queryType,
  //   notifications: summary.notifications,
  //   server: {
  //     address: summary.server.address,
  //   },
  //   database: summary.database,
  //   costEstimate: metadata.cost_estimate ?? 0,
  //   parsingTime: metadata.parsing_time ?? 0,
  //   planExecutionTime: metadata.plan_execution_time ?? 0,
  //   planningTime: metadata.planning_time ?? 0,
  //   runId: metadata.run_id
  //   stats: summary.counters._stats,
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
      onNext: (record) => {
        result.records.push(parseRecord(record));
      },
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
