#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "admin", "Username for the database");
DEFINE_string(password, "admin", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

/**
 * Verifies that user 'user' has privileges that are given as positional
 * arguments.
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

  try {
    auto ret = client.Execute("SHOW PRIVILEGES FOR user", {});
    const auto &records = ret.records;
    uint64_t count_got = 0;
    for (const auto &record : records) {
      count_got += record.size();
    }
    if (count_got != argc - 1) {
      LOG(FATAL) << "Expected the grants to have " << argc - 1
                 << " entries but they had " << count_got << " entries!";
    }
    uint64_t pos = 1;
    for (const auto &record : records) {
      for (const auto &value : record) {
        std::string expected(argv[pos++]);
        if (value.ValueString() != expected) {
          LOG(FATAL) << "Expected to get the value '" << expected
                     << " but got the value '" << value.ValueString() << "'";
        }
      }
    }
  } catch (const communication::bolt::ClientQueryException &e) {
    LOG(FATAL) << "The query shoudn't have failed but it failed with an "
                  "error message '"
               << e.what() << "'";
  }

  return 0;
}
