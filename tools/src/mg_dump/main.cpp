#include <exception>
#include <filesystem>
#include <fstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "version.hpp"

namespace fs = std::filesystem;

const char *kUsage =
    "Memgraph dump tool.\n"
    "A simple tool for dumping Memgraph database's data as list of openCypher "
    "queries.\n";

DEFINE_string(host, "127.0.0.1",
              "Server address. It can be a DNS resolvable hostname.");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, true, "Use SSL when connecting to the server");

DECLARE_int32(min_log_level);

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char **argv) {
  gflags::SetVersionString(version_string);
  gflags::SetUsageMessage(kUsage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // TODO(tsabolcec): `FLAGS_min_log_level` is here to silent logs from
  // `communication::bolt::Client`. Remove this setting once we move to C
  // mg-bolt library which doesn't use glog.
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_host),
                                 FLAGS_port);
  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  try {
    const std::string bolt_client_version =
        fmt::format("mg_dump/{}", gflags::VersionString());
    client.Connect(endpoint, FLAGS_username, FLAGS_password,
                   bolt_client_version);
  } catch (const communication::bolt::ClientFatalException &e) {
    std::cerr << "Connection failed: " << e.what() << std::endl;
    return 1;
  }

  try {
    auto ret = client.Execute("DUMP DATABASE", {});
    if (ret.fields.size() != 1) {
      std::cerr << "Error: client received response in unexpected format"
                << std::endl;
      return 1;
    }

    for (const auto &row : ret.records) {
      CHECK(row.size() == 1U) << "Unexpected number of columns in a row";
      std::cout << row[0].ValueString() << std::endl;
    }
  } catch (const communication::bolt::ClientFatalException &e) {
    std::cerr << "Client received exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
