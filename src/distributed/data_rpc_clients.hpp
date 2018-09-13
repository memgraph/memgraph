/// @file

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace distributed {

class RpcWorkerClients;

template <typename TRecord>
struct RemoteElementInfo {
  RemoteElementInfo() = delete;
  RemoteElementInfo(const RemoteElementInfo &) = delete;
  // TODO (buda): The default move constructor should be deleted but it seems
  // that clang-3.9 doesn't know how to do RVO when this struct is used.
  RemoteElementInfo(RemoteElementInfo &&) = default;
  RemoteElementInfo &operator=(const RemoteElementInfo &) = delete;
  RemoteElementInfo &operator=(RemoteElementInfo &&) = delete;

  RemoteElementInfo(int64_t cypher_id, std::unique_ptr<TRecord> record_ptr)
      : cypher_id(cypher_id), record_ptr(std::move(record_ptr)) {}

  int64_t cypher_id;
  std::unique_ptr<TRecord> record_ptr;
};

/// Provides access to other worker's data.
class DataRpcClients {
 public:
  DataRpcClients(RpcWorkerClients &clients) : clients_(clients) {}

  /// Returns a remote worker's record (vertex/edge) data for the given params.
  /// That worker must own the vertex/edge for the given id, and that vertex
  /// must be visible in given transaction.
  template <typename TRecord>
  RemoteElementInfo<TRecord> RemoteElement(int worker_id,
                                           tx::TransactionId tx_id,
                                           gid::Gid gid);

  /// Returns (worker_id, vertex_count) for each worker and the number of
  /// vertices on it from the perspective of transaction `tx_id`.
  std::unordered_map<int, int64_t> VertexCounts(tx::TransactionId tx_id);

 private:
  RpcWorkerClients &clients_;
};

}  // namespace distributed
