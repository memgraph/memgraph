#pragma once

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "communication/bolt/client.hpp"

namespace communication::bolt {

/// HA Bolt client.
/// It has methods used to execute queries against a cluster of servers. It
/// supports both SSL and plaintext connections.
class HAClient final {
 public:
  HAClient(const std::vector<io::network::Endpoint> &endpoints,
           communication::ClientContext *context, const std::string &username,
           const std::string &password, uint64_t num_retries,
           const std::chrono::milliseconds &retry_delay,
           const std::string &client_name = "memgraph-bolt")
      : endpoints_(endpoints),
        context_(context),
        username_(username),
        password_(password),
        num_retries_(num_retries),
        retry_delay_(retry_delay),
        client_name_(client_name) {
    if (endpoints.size() < 3) {
      throw ClientFatalException(
          "You should specify at least three server endpoints to connect to!");
    }
    // Create all clients.
    for (size_t i = 0; i < endpoints.size(); ++i) {
      clients_.push_back(std::make_unique<Client>(context_));
    }
  }

  HAClient(const HAClient &) = delete;
  HAClient(HAClient &&) = delete;
  HAClient &operator=(const HAClient &) = delete;
  HAClient &operator=(HAClient &&) = delete;

  /// Function used to execute queries against the leader server.
  /// @throws ClientQueryException when there is some transient error while
  ///                              executing the query (eg. mistyped query,
  ///                              etc.)
  /// @throws ClientFatalException when we couldn't communicate with the leader
  ///                              server even after `num_retries` tries
  QueryData Execute(const std::string &query,
                    const std::map<std::string, Value> &parameters) {
    for (int i = 0; i < num_retries_; ++i) {
      // Try to find a leader.
      if (!leader_) {
        for (int j = 0; j < num_retries_; ++j) {
          if (!(i == 0 && j == 0)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retry_delay_));
          }
          try {
            FindLeader();
            break;
          } catch (const ClientFatalException &e) {
            continue;
          }
        }
        if (!leader_) {
          throw ClientFatalException("Couldn't find leader after {} tries!",
                                     num_retries_);
        }
      }
      // Try to execute the query.
      try {
        return leader_->Execute(query, parameters);
      } catch (const utils::BasicException &e) {
        // Check if this is a cluster failure or a Raft failure.
        auto qe = dynamic_cast<const ClientQueryException *>(&e);
        if (dynamic_cast<const ClientFatalException *>(&e) ||
            (qe && qe->code() == "Memgraph.DatabaseError.Raft.Error")) {
          // We need to look for a new leader.
          leader_ = nullptr;
          continue;
        }
        // If it isn't just forward the exception to the client.
        throw;
      }
    }
    throw ClientFatalException("Couldn't execute query after {} tries!",
                               num_retries_);
  }

  /// Function that returns the current leader ID.
  ///
  /// @throws ClientFatalException when we couldn't find the leader server even
  ///                              after `num_retries` tries
  uint64_t GetLeaderId() {
    Execute("SHOW RAFT INFO", {});
    return leader_id_;
  }

 private:
  void FindLeader() {
    // Reconnect clients that aren't available
    bool connected = false;
    for (size_t i = 0; i < clients_.size(); ++i) {
      const auto &ep = endpoints_[i];
      const auto &client = clients_[i];
      try {
        client->Execute("SHOW RAFT INFO", {});
        connected = true;
        continue;
      } catch (const ClientQueryException &e) {
        continue;
      } catch (const ClientFatalException &e) {
        client->Close();
        try {
          client->Connect(ep, username_, password_, client_name_);
          connected = true;
        } catch (const utils::BasicException &) {
          // Suppress any exceptions.
        }
      }
    }
    if (!connected) {
      throw ClientFatalException("Couldn't connect to any server!");
    }

    // Determine which server is the leader
    leader_ = nullptr;
    uint64_t leader_id = 0;
    int64_t leader_term = -1;
    for (uint64_t i = 0; i < clients_.size(); ++i) {
      auto &client = clients_[i];
      try {
        auto ret = client->Execute("SHOW RAFT INFO", {});
        int64_t term_id = -1;
        bool is_leader = false;
        for (const auto &rec : ret.records) {
          if (rec.size() != 2) continue;
          if (!rec[0].IsString()) continue;
          const auto &key = rec[0].ValueString();
          if (key == "term_id") {
            if (!rec[1].IsInt()) continue;
            term_id = rec[1].ValueInt();
          } else if (key == "is_leader") {
            if (!rec[1].IsBool()) continue;
            is_leader = rec[1].ValueBool();
          } else {
            continue;
          }
        }
        if (is_leader && term_id > leader_term) {
          leader_term = term_id;
          leader_id = i + 1;
          leader_ = client.get();
        }
      } catch (const utils::BasicException &) {
        continue;
      }
    }
    leader_id_ = leader_id;
    if (!leader_) {
      throw ClientFatalException("Couldn't find leader server!");
    }
  }

  std::vector<io::network::Endpoint> endpoints_;
  communication::ClientContext *context_;
  std::string username_;
  std::string password_;
  uint64_t num_retries_;
  std::chrono::milliseconds retry_delay_;
  std::string client_name_;

  uint64_t leader_id_ = 0;
  Client *leader_ = nullptr;
  std::vector<std::unique_ptr<Client>> clients_;
};
}  // namespace communication::bolt
