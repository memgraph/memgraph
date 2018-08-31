/// @file
#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/counters.hpp"

namespace communication::rpc {
class Server;
class ClientPool;
}  // namespace communication::rpc

namespace database {

/// Implementation for distributed master
class MasterCounters : public Counters {
 public:
  explicit MasterCounters(communication::rpc::Server *server);

  int64_t Get(const std::string &name) override;
  void Set(const std::string &name, int64_t value) override;

 private:
  communication::rpc::Server *rpc_server_;
  ConcurrentMap<std::string, std::atomic<int64_t>> counters_;
};

/// Implementation for distributed worker
class WorkerCounters : public Counters {
 public:
  explicit WorkerCounters(communication::rpc::ClientPool *master_client_pool);

  int64_t Get(const std::string &name) override;
  void Set(const std::string &name, int64_t value) override;

 private:
  communication::rpc::ClientPool *master_client_pool_;
};

}  // namespace database
