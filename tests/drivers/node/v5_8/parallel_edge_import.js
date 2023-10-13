var neo4j = require('neo4j-driver');
var driver = neo4j.driver("bolt://localhost:7687",
                          neo4j.auth.basic("", ""),
                          { encrypted: 'ENCRYPTION_OFF', maxTransactionRetryTime: 30000 });
var session = driver.session();

var node_count = 50;

function die() {
  session.close();
  driver.close();
  process.exit(1);
}

async function run_query(query, callback) {
  var run = session.run(query, {});
  run.then(callback).catch(function (error) {
    console.log(error);
    die();
  });
}

async function run_query_with_sess(sess, query, callback) {
  const result = await sess.executeWrite(async tx => {
    return await tx.run(query);
  });
  console.log("Transaction got through!");
  sess.close();
  return result;
}

async function run_conflicting_queries(query, number, callback) {
  const promises = [];
  for (let i = 0; i < number; i++) {
    var sess = driver.session();
    promises.push(run_query_with_sess(sess, query));
  }
  await Promise.all(promises).then(callback);
}

async function assert_node_count(expectedCount) {
  try {
    const run = await session.run("MATCH (n) RETURN count(n) AS cnt", {});
    const result = run.records[0].get("cnt").toNumber();
    if (result !== expectedCount) {
      console.log("Count result is not correct! (" + result + ")");
      die();
    } else {
      console.log("Count result is correct! (" + result + ")");
    }
  } catch (error) {
    console.log(error);
    die();
  } finally {
    session.close();
    driver.close();
  }
}

run_query("MATCH (n) DETACH DELETE n;", function (result) {
  console.log("Database cleared.");
  run_query("MERGE (root:Root);", function (result) {
    console.log("Root created.");
    run_conflicting_queries("MATCH (root:Root) CREATE (n:Node) CREATE (n)-[:TROUBLING_EDGE]->(root)", node_count - 1, function (result) {
      assert_node_count(node_count);
    });
  });
});
