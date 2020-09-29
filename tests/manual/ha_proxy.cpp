#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/ha_client.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/server.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/flag_validation.hpp"
#include "utils/signals.hpp"
#include "utils/string.hpp"
#include "version.hpp"

DEFINE_string(address, "127.0.0.1", "Proxy server listen address.");
DEFINE_int32(port, 7687, "Proxy server listen port.");
DEFINE_string(cert_file, "", "Proxy server SSL certificate file.");
DEFINE_string(key_file, "", "Proxy server SSL key file.");
DEFINE_VALIDATED_int32(num_workers,
                       std::max(std::thread::hardware_concurrency(), 1U),
                       "Proxy server number of workers (Bolt).",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(session_inactivity_timeout, 1800,
                       "Proxy server time in seconds after which inactive "
                       "sessions will be closed.",
                       FLAG_IN_RANGE(1, INT32_MAX));

DEFINE_string(endpoints, "",
              "Cluster server endpoints (host:port, separated by comma).");
DEFINE_bool(use_ssl, true,
            "Set to true to connect with SSL to the cluster servers.");

DEFINE_int32(num_retries, 3,
             "Number of retries for each operation (execute/connect).");
DEFINE_int32(retry_delay_ms, 1000, "Delay before retrying (in ms).");

// Global data state that is used by the BoltSession.
struct SessionData {
  std::vector<io::network::Endpoint> endpoints;
};

class BoltSession final
    : public communication::bolt::Session<communication::InputStream,
                                          communication::OutputStream> {
 public:
  BoltSession(SessionData *data, const io::network::Endpoint &endpoint,
              communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>(
            input_stream, output_stream),
        session_data_(data),
        endpoint_(endpoint),
        context_(FLAGS_use_ssl) {}

  using communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>::TEncoder;

  std::vector<std::string> Interpret(
      const std::string &query,
      const std::map<std::string, communication::bolt::Value> &params)
      override {
    records_ = {};
    metadata_ = {};
    try {
      auto ret = client_->Execute(query, params);
      records_ = std::move(ret.records);
      metadata_ = std::move(ret.metadata);
      return ret.fields;
    } catch (const communication::bolt::ClientQueryException &e) {
      // Wrap query exceptions in a client error to indicate to the client that
      // it should fix the query and try again.
      throw communication::bolt::ClientError(e.what());
    } catch (const communication::bolt::ClientFatalException &e) {
      // Wrap fatal exceptions in a verbose error to indcate to the client that
      // something is wrong with the database.
      throw communication::bolt::VerboseError(
          communication::bolt::VerboseError::Classification::DATABASE_ERROR,
          "HighAvailability", "Error", e.what());
    }
  }

  std::map<std::string, communication::bolt::Value> Pull(TEncoder *encoder,
                                                         int) override {
    for (const auto &record : records_) {
      encoder->MessageRecord(record);
    }
    return metadata_;
  }

  void Abort() override {
    // Called only for cleanup.
    records_.clear();
    metadata_.clear();
  }

  bool Authenticate(const std::string &username,
                    const std::string &password) override {
    client_ = std::make_unique<communication::bolt::HAClient>(
        session_data_->endpoints, &context_, username, password,
        FLAGS_num_retries, std::chrono::milliseconds(FLAGS_retry_delay_ms),
        fmt::format("memgraph_ha_proxy/{}", version_string));
    return true;
  }

  std::optional<std::string> GetServerNameForInit() override {
    return std::nullopt;
  }

 private:
  SessionData *session_data_;
  io::network::Endpoint endpoint_;

  communication::ClientContext context_;
  std::unique_ptr<communication::bolt::HAClient> client_;

  std::vector<std::vector<communication::bolt::Value>> records_;
  std::map<std::string, communication::bolt::Value> metadata_;
};

// Needed to correctly handle proxy destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main and that would cause a crash.
volatile sig_atomic_t is_shutting_down = 0;

void InitSignalHandlers(const std::function<void()> &shutdown_fun) {
  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  // Wrap the shutdown function in a safe way to prevent recursive shutdown.
  auto shutdown = [shutdown_fun]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    shutdown_fun();
  };

  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate,
                                              shutdown, block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                              block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Setup SIGUSR1 to be used for reopening log files, when e.g. logrotate
  // rotates our logs.
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::User1, []() {
    google::CloseLogDestination(google::INFO);
  })) << "Unable to register SIGUSR1 handler!";
}

std::vector<io::network::Endpoint> GetEndpoints() {
  std::vector<io::network::Endpoint> ret;
  for (const auto &endpoint : utils::Split(FLAGS_endpoints, ",")) {
    auto split = utils::Split(utils::Trim(endpoint), ":");
    CHECK(split.size() == 2) << "Invalid endpoint!";
    ret.emplace_back(
        io::network::ResolveHostname(std::string(utils::Trim(split[0]))),
        static_cast<uint16_t>(std::stoi(std::string(utils::Trim(split[1])))));
  }
  return ret;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  communication::ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_key_file.empty() && !FLAGS_cert_file.empty()) {
    context = communication::ServerContext(FLAGS_key_file, FLAGS_cert_file);
    service_name = "BoltS";
  }

  SessionData session_data{GetEndpoints()};
  communication::Server<BoltSession, SessionData> server(
      {FLAGS_address, static_cast<uint16_t>(FLAGS_port)}, &session_data,
      &context, FLAGS_session_inactivity_timeout, service_name,
      FLAGS_num_workers);

  // Handler for regular termination signals
  InitSignalHandlers([&server] { server.Shutdown(); });

  CHECK(server.Start()) << "Couldn't start the Bolt server!";
  server.AwaitShutdown();

  return 0;
}
