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
  var run = session.run(query, {});
  run.then(callback).catch(function (error) {
    console.log(error);
    die();
  });
}

run_query("MATCH (n) DETACH DELETE n;", function (result) {
  console.log("Database cleared.");
  run_query("CREATE (alice:Person {name: 'Alice', age: 22});", function (result) {
    console.log("Record created.");
    run_query("MATCH (n) RETURN n", function (result) {
      console.log("Record matched.");
      const alice = result.records[0].get("n");
      const label = alice.labels[0];
      const name = alice.properties["name"];
      const age = alice.properties["age"];
      if(label != "Person" || name != "Alice" || age != 22){
        console.log("Data doesn't match!");
        die();
      }
      console.log("Label: " + label);
      console.log("name: " + name);
      console.log("age: " + age);
      console.log("All ok!");
      driver.close();
    });
  });
});
