#pragma once

#include <fstream>
#include <vector>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "io/network/endpoint.hpp"
#include "utils/logging.hpp"

using EndpointT = io::network::Endpoint;
using ContextT = communication::ClientContext;
using ClientT = communication::bolt::Client;
using QueryDataT = communication::bolt::QueryData;
using communication::bolt::Value;

class BoltClient {
 public:
  BoltClient(const std::string &address, uint16_t port, const std::string &username, const std::string &password,
             const std::string & = "", bool use_ssl = false)
      : context_(use_ssl), client_(context_) {
    EndpointT endpoint(address, port);

    if (!client_.Connect(endpoint, username, password)) {
      LOG_FATAL("Could not connect to: {}", endpoint);
    }
  }

  QueryDataT Execute(const std::string &query, const std::map<std::string, Value> &parameters) {
    return client_.Execute(query, parameters);
  }

  void Close() { client_.Close(); }

 private:
  ContextT context_;
  ClientT client_;
};
