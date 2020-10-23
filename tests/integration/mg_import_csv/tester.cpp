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

/**
 * Executes "DUMP DATABASE" and outputs all results to stdout. On any errors it
 * exits with a non-zero exit code.
 */
// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::SSLInit sslInit;

  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_address),
                                 FLAGS_port);

  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);
  auto ret = client.Execute("DUMP DATABASE", {});
  for (const auto &row : ret.records) {
    CHECK(row.size() == 1) << "Too much entries in query dump row (got "
                           << row.size() << ", expected 1)!";
    std::cout << row[0].ValueString() << std::endl;
  }

  return 0;
}
