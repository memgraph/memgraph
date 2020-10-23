// Determines how long could be a query executed
// from JavaScript driver.
//
// Performs binary search until the maximum possible
// query size has found.

// init driver
var neo4j = require('neo4j-driver');
var driver = neo4j.driver("bolt://localhost:7687",
                          neo4j.auth.basic("", ""),
                          { encrypted: 'ENCRYPTION_OFF' });

// init state
var property_size = 0;
var min_len = 1;
var max_len = 1000000;

// hacking with JS and callbacks concept
function serial_execution() {
  var next_size = [Math.floor((min_len + max_len) / 2)];
  setInterval(function() {
    if (next_size.length > 0) {
      property_size = next_size.pop();
      var query = "CREATE (n {name:\"" +
        (new Array(property_size)).join("a")+ "\"})";
      var session = driver.session();
      session.run(query, {}).then(function (result) {
        console.log("Success with the query length " + query.length);
        if (min_len == max_len || property_size + 1 > max_len) {
          console.log("\nThe max length of a query from JS driver is: " +
            query.length + "\n");
          session.close();
          driver.close();
          process.exit(0);
        }
        min_len = property_size + 1;
        next_size.push(Math.floor((min_len + max_len) / 2));
      }).catch(function (error) {
        console.log("Failure with the query length " + query.length);
        max_len = property_size - 1;
        next_size.push(Math.floor((min_len + max_len) / 2));
      }).then(function(){
        session.close();
      });
    }
  }, 100);
}

// execution
console.log("\nDetermine how long can be a query sent from JavaScript driver.");
serial_execution(); // I don't like JavaScript
