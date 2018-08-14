#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_string(step, "", "Step that should be executed on the database.");

void ExecuteQuery(communication::bolt::Client &client,
                  const std::string &query) {
  try {
    client.Execute(query, {});
  } catch (const communication::bolt::ClientQueryException &e) {
    LOG(FATAL) << "Couldn't execute query '" << query
               << "'! Received exception: " << e.what();
  }
}

void ExecuteQueryAndCheck(communication::bolt::Client &client,
                          const std::string &query, int64_t value) {
  try {
    auto resp = client.Execute(query, {});
    if (resp.records.size() == 0 || resp.records[0].size() == 0) {
      LOG(FATAL) << "The query '" << query << "' didn't return records!";
    }
    if (resp.records[0][0].ValueInt() != value) {
      LOG(FATAL) << "The query '" << query << "' was expected to return "
                 << value << " but it returned "
                 << resp.records[0][0].ValueInt() << "!";
    }
  } catch (const communication::bolt::ClientQueryException &e) {
    LOG(FATAL) << "Couldn't execute query '" << query
               << "'! Received exception: " << e.what();
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_address),
                                 FLAGS_port);

  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  if (!client.Connect(endpoint, FLAGS_username, FLAGS_password)) {
    LOG(FATAL) << "Couldn't connect to server " << FLAGS_address << ":"
               << FLAGS_port;
  }

  if (FLAGS_step == "start") {
    ExecuteQuery(client,
                 "CREATE STREAM strim AS LOAD DATA KAFKA '127.0.0.1:9092' WITH "
                 "TOPIC 'test' WITH TRANSFORM "
                 "'http://127.0.0.1:8000/transform.py'");
    ExecuteQuery(client, "START STREAM strim");
  } else if (FLAGS_step == "verify") {
    ExecuteQueryAndCheck(client,
                         "UNWIND RANGE(1, 4) AS x MATCH (n:node {num: "
                         "toString(x)}) RETURN count(n)",
                         4);
    ExecuteQueryAndCheck(client,
                         "UNWIND [[1, 2], [3, 4], [1, 4]] AS x MATCH (n:node "
                         "{num: toString(x[0])})-[e:et]-(m:node {num: "
                         "toString(x[1])}) RETURN count(e)",
                         3);

  } else {
    LOG(FATAL) << "Unknown step " << FLAGS_step << "!";
  }

  return 0;
}
