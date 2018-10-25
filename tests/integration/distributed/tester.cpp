#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_string(step, "", "The step to execute (available: create, execute)");

/**
 * This test creates a sample dataset in the database and then executes a query
 * that has a long execution time so that we can see what happens if the cluster
 * dies mid-execution.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_address),
                                 FLAGS_port);

  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  if (FLAGS_step == "create") {
    client.Execute("UNWIND range(0, 10000) AS x CREATE ()", {});
  } else if (FLAGS_step == "execute") {
    try {
      client.Execute("MATCH (a), (b), (c), (d), (e), (f) RETURN COUNT(*)", {});
      LOG(FATAL)
          << "The long query shouldn't have finished successfully, but it did!";
    } catch (const communication::bolt::ClientQueryException &e) {
      LOG(WARNING) << e.what();
    } catch (const communication::bolt::ClientFatalException &) {
      LOG(WARNING) << "The server closed the connection to us!";
    }
  } else {
    LOG(FATAL) << "Unknown step!";
  }

  return 0;
}
