#include <fstream>
#include <vector>

#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using ClientT = communication::bolt::Client<SocketT>;
using QueryDataT = communication::bolt::QueryData;
using communication::bolt::DecodedValue;

class BoltClient {
 public:
  BoltClient(std::string &address, std::string &port, std::string &username,
             std::string &password, std::string database = "") {
    SocketT socket;
    EndpointT endpoint;

    try {
      endpoint = EndpointT(address, port);
    } catch (const io::network::NetworkEndpointException &e) {
      LOG(FATAL) << "Invalid address or port: " << address << ":" << port;
    }
    if (!socket.Connect(endpoint)) {
      LOG(FATAL) << "Could not connect to: " << address << ":" << port;
    }

    client_ = std::make_unique<ClientT>(std::move(socket), username, password);
  }

  QueryDataT Execute(const std::string &query,
                     const std::map<std::string, DecodedValue> &parameters) {
    return client_->Execute(query, parameters);
  }

  void Close() { client_->Close(); }

 private:
  std::unique_ptr<ClientT> client_;
};
