#include <atomic>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/client.hpp"
#include "communication/server.hpp"
#include "utils/exceptions.hpp"

DEFINE_string(server_cert_file, "", "Server certificate file to use.");
DEFINE_string(server_key_file, "", "Server key file to use.");
DEFINE_string(server_ca_file, "", "Server CA file to use.");
DEFINE_bool(server_verify_peer, false, "Set to true to verify the peer.");

DEFINE_string(client_cert_file, "", "Client certificate file to use.");
DEFINE_string(client_key_file, "", "Client key file to use.");

const std::string message = "ssl echo test";

struct EchoData {};

class EchoSession {
 public:
  EchoSession(EchoData *, const io::network::Endpoint &,
              communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    if (input_stream_->size() < message.size()) return;
    LOG(INFO) << "Server received message.";
    if (!output_stream_->Write(input_stream_->data(), message.size())) {
      throw utils::BasicException("Output stream write failed!");
    }
    LOG(INFO) << "Server sent message.";
    input_stream_->Shift(message.size());
  }

 private:
  communication::InputStream *input_stream_;
  communication::OutputStream *output_stream_;
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Initialize the communication stack.
  communication::Init();

  // Initialize the server.
  EchoData echo_data;
  communication::ServerContext server_context(
      FLAGS_server_key_file, FLAGS_server_cert_file, FLAGS_server_ca_file,
      FLAGS_server_verify_peer);
  communication::Server<EchoSession, EchoData> server(
      {"127.0.0.1", 0}, &echo_data, &server_context, -1, "SSL", 1);

  // Initialize the client.
  communication::ClientContext client_context(FLAGS_client_key_file,
                                              FLAGS_client_cert_file);
  communication::Client client(&client_context);

  // Connect to the server.
  CHECK(client.Connect(server.endpoint())) << "Couldn't connect to server!";

  // Perform echo.
  CHECK(client.Write(message)) << "Client couldn't send message!";
  LOG(INFO) << "Client sent message.";
  CHECK(client.Read(message.size())) << "Client couldn't receive message!";
  LOG(INFO) << "Client received message.";
  CHECK(std::string(reinterpret_cast<const char *>(client.GetData()),
                    message.size()) == message)
      << "Received message isn't equal to sent message!";

  // Shutdown the server.
  server.Shutdown();
  server.AwaitShutdown();

  return 0;
}
