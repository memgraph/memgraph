#pragma once

#include "memgraph_config.hpp"

#include "reactors_distributed.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

class MemgraphDistributed {
 private:
  using Location = std::pair<std::string, uint16_t>;

 public:
  /**
   * Get the (singleton) instance of MemgraphDistributed.
   */
  static MemgraphDistributed &GetInstance() {
    static MemgraphDistributed memgraph;
    return memgraph;
  }

  EventStream *FindChannel(MnidT mnid, const std::string &reactor,
                           const std::string &channel) {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto &location = mnodes_.at(mnid);
    return Distributed::GetInstance().FindChannel(
        location.first, location.second, reactor, channel);
  }

  void RegisterConfig(const Config &config) {
    config_ = config;
    for (auto &node : config_.nodes) {
      RegisterMemgraphNode(node.mnid, node.address, node.port);
    }
  }

  std::vector<MnidT> GetAllMnids() {
    std::vector<MnidT> mnids;
    for (auto &node : config_.nodes) {
      mnids.push_back(node.mnid);
    }
    return mnids;
  }

  /**
   * The leader is currently the first node in the config.
   */
  MnidT LeaderMnid() const { return config_.nodes.front().mnid; }

 protected:
  MemgraphDistributed() {}

  /** Register memgraph node id to the given location. */
  void RegisterMemgraphNode(MnidT mnid, const std::string &address,
                            uint16_t port) {
    std::unique_lock<std::mutex> lock(mutex_);
    mnodes_[mnid] = Location(address, port);
  }

 private:
  Config config_;

  std::mutex mutex_;
  std::unordered_map<MnidT, Location> mnodes_;

  MemgraphDistributed(const MemgraphDistributed &) = delete;
  MemgraphDistributed(MemgraphDistributed &&) = delete;
  MemgraphDistributed &operator=(const MemgraphDistributed &) = delete;
  MemgraphDistributed &operator=(MemgraphDistributed &&) = delete;
};
