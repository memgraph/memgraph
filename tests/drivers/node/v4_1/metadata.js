var neo4j = require('neo4j-driver');
var driver = neo4j.driver("bolt://localhost:7687",
                          neo4j.auth.basic("", ""),
                          { encrypted: 'ENCRYPTION_OFF' });
var session = driver.session();

function die() {
  session.close();
  driver.close();
  process.exit(1);
}

function run_query(query, callback) {
  var run = session.run(query, {}, {metadata:{"ver":"session", "str":"aha", "num":123}});
  run.then(callback).catch(function (error) {
    console.log(error);
    die();
  });
}

console.log("Checking implicit transaction metadata...");
run_query("SHOW TRANSACTIONS;", function (result) {
  const md = result.records[0].get("metadata");
  if (md["ver"] != "session" || md["str"] != "aha" || md["num"] != 123){
    console.log("Error while reading metadata!");
    die();
  }
  console.log("All ok!");
  session.close();
  driver.close();
});
