#pragma once 

#include <fstream>
#include <vector>

#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::Endpoint;
using ClientT = communication::bolt::Client<SocketT>;
using QueryDataT = communication::bolt::QueryData;
using communication::bolt::DecodedValue;

class BoltClient {
 public:
  BoltClient(const std::string &address, uint16_t port,
             const std::string &username, const std::string &password,
             const std::string & = "") {
    SocketT socket;
    EndpointT endpoint(address, port);

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
