/// @file
#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/distributed/counters.hpp"
#include "distributed/coordination.hpp"

namespace database {

/// Implementation for distributed master
class MasterCounters : public Counters {
 public:
  explicit MasterCounters(distributed::Coordination *coordination);

  int64_t Get(const std::string &name) override;
  void Set(const std::string &name, int64_t value) override;

 private:
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
