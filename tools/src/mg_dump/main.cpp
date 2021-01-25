#include <iostream>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <mgclient.h>
#include <spdlog/spdlog.h>

#include "version.hpp"

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

int main(int argc, char **argv) {
  gflags::SetVersionString(version_string);
  gflags::SetUsageMessage(kUsage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  const std::string bolt_client_version =
      fmt::format("mg_dump/{}", gflags::VersionString());

  // Setup session params
  mg_session_params *params = mg_session_params_make();
  if (!params) {
    std::cerr << "Failed to allocate session params" << std::endl;
    return 1;
  }
  mg_session_params_set_host(params, FLAGS_host.c_str());
  mg_session_params_set_port(params, FLAGS_port);
  if (!FLAGS_username.empty()) {
    mg_session_params_set_username(params, FLAGS_username.c_str());
    mg_session_params_set_password(params, FLAGS_password.c_str());
  }
  mg_session_params_set_user_agent(params, bolt_client_version.c_str());
  mg_session_params_set_sslmode(
      params, FLAGS_use_ssl ? MG_SSLMODE_REQUIRE : MG_SSLMODE_DISABLE);

  // Establish connection
  mg_session *session = nullptr;
  int status = mg_connect(params, &session);
  mg_session_params_destroy(params);
  if (status < 0) {
    std::cerr << "Connection failed: " << mg_session_error(session)
              << std::endl;
    mg_session_destroy(session);
    return 1;
  }

  if (mg_session_run(session, "DUMP DATABASE", nullptr, nullptr, nullptr,
                     nullptr) < 0) {
    std::cerr << "Execution failed: " << mg_session_error(session) << std::endl;
    mg_session_destroy(session);
    return 1;
  }

  if (mg_session_pull(session, nullptr) < 0) {
    std::cerr << "Execution failed: " << mg_session_error(session) << std::endl;
    mg_session_destroy(session);
    return 1;
  }

  // Fetch results
  mg_result *result;
  while ((status = mg_session_fetch(session, &result)) == 1) {
    const mg_list *row = mg_result_row(result);
    if (mg_list_size(row) != 1) {
      spdlog::critical("Error: dump client received data in unexpected format");
      std::abort();
    }
    const mg_value *value = mg_list_at(row, 0);
    if (mg_value_get_type(value) != MG_VALUE_TYPE_STRING) {
      spdlog::critical("Error: dump client received data in unexpected format");
      std::abort();
    };
    const mg_string *str_value = mg_value_string(value);
    std::cout.write(mg_string_data(str_value), mg_string_size(str_value));
    std::cout << std::endl;  // `std::endl` flushes
  }

  if (status < 0) {
    std::cerr << "Execution failed: " << mg_session_error(session) << std::endl;
    return 1;
  }

  mg_session_destroy(session);

  return 0;
}
