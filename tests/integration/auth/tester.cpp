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

DEFINE_bool(check_failure, false, "Set to true to enable failure checking.");
DEFINE_bool(should_fail, false, "Set to true to expect a failure.");
DEFINE_string(failure_message, "", "Set to the expected failure message.");

/**
 * Executes queries passed as positional arguments and verifies whether they
 * succeeded, failed, failed with a specific error message or executed without a
 * specific error occurring.
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

  for (int i = 1; i < argc; ++i) {
    std::string query(argv[i]);
    try {
      client.Execute(query, {});
    } catch (const communication::bolt::ClientQueryException &e) {
      if (!FLAGS_check_failure) {
        if (!FLAGS_failure_message.empty() &&
            e.what() == FLAGS_failure_message) {
          LOG(FATAL)
              << "The query should have succeeded or failed with an error "
                 "message that isn't equal to '"
              << FLAGS_failure_message
              << "' but it failed with that error message";
        }
        continue;
      }
      if (FLAGS_should_fail) {
        if (!FLAGS_failure_message.empty() &&
            e.what() != FLAGS_failure_message) {
          LOG(FATAL)
              << "The query should have failed with an error message of '"
              << FLAGS_failure_message << "' but instead it failed with '"
              << e.what() << "'";
        }
        return 0;
      } else {
        LOG(FATAL) << "The query shoudn't have failed but it failed with an "
                      "error message '"
                   << e.what() << "'";
      }
    }
    if (!FLAGS_check_failure) continue;
    if (FLAGS_should_fail) {
      LOG(FATAL) << "The query should have failed but instead it executed "
                    "successfully!";
    }
  }

  return 0;
}
