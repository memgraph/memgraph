#include <atomic>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/server.hpp"
#include "utils/exceptions.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 54321, "Server port");
DEFINE_string(cert_file, "", "Certificate file to use.");
DEFINE_string(key_file, "", "Key file to use.");
DEFINE_string(ca_file, "", "CA file to use.");
DEFINE_bool(verify_peer, false, "Set to true to verify the peer.");

struct EchoData {
  std::atomic<bool> alive{true};
};

class EchoSession {
 public:
  EchoSession(EchoData &data, communication::InputStream &input_stream,
              communication::OutputStream &output_stream)
      : data_(data),
        input_stream_(input_stream),
        output_stream_(output_stream) {}

  void Execute() {
    if (input_stream_.size() < 2) return;
    const uint8_t *data = input_stream_.data();
    uint16_t size = *reinterpret_cast<const uint16_t *>(input_stream_.data());
    input_stream_.Resize(size + 2);
    if (input_stream_.size() < size + 2) return;
    if (size == 0) {
      LOG(INFO) << "Server received EOF message";
      data_.alive.store(false);
      return;
    }
    LOG(INFO) << "Server received '"
              << std::string(reinterpret_cast<const char *>(data + 2), size)
              << "'";
    if (!output_stream_.Write(data + 2, size)) {
      throw utils::BasicException("Output stream write failed!");
    }
    input_stream_.Shift(size + 2);
  }

 private:
  EchoData &data_;
  communication::InputStream &input_stream_;
  communication::OutputStream &output_stream_;
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  // Initialize the server.
  EchoData echo_data;
  io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
  communication::ServerContext context(FLAGS_key_file, FLAGS_cert_file,
                                       FLAGS_ca_file, FLAGS_verify_peer);
  communication::Server<EchoSession, EchoData> server(endpoint, echo_data,
                                                      &context, -1, "SSL", 1);

  while (echo_data.alive) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return 0;
}
