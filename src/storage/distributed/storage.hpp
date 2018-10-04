#pragma once

#include <experimental/filesystem>
#include <experimental/optional>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "mvcc/distributed/version_list.hpp"
#include "storage/common/types.hpp"
#include "storage/distributed/address.hpp"
#include "storage/distributed/edge.hpp"
#include "storage/distributed/indexes/key_index.hpp"
#include "storage/distributed/indexes/label_property_index.hpp"
#include "storage/distributed/vertex.hpp"
#include "storage/kvstore/kvstore.hpp"
#include "transactions/type.hpp"

namespace distributed {
class IndexRpcServer;
};

namespace database {
class GraphDb;
};

namespace database {

/** A data structure containing the main data members of a graph database. */
class Storage {
 public:
  Storage(int worker_id, const std::vector<std::string> &properties_on_disk)
      : worker_id_(worker_id),
        vertex_generator_{worker_id},
        edge_generator_{worker_id},
        properties_on_disk_{properties_on_disk} {}

 public:
  ~Storage() {
    // Delete vertices and edges which weren't collected before, also deletes
    // records inside version list
    for (auto &id_vlist : vertices_.access()) delete id_vlist.second;
    for (auto &id_vlist : edges_.access()) delete id_vlist.second;
  }

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  gid::Generator &VertexGenerator() { return vertex_generator_; }
  gid::Generator &EdgeGenerator() { return edge_generator_; }
  LabelPropertyIndex &label_property_index() { return label_property_index_; }

  /// Gets the local address for the given gid. Fails if not present.
  template <typename TRecord>
  mvcc::VersionList<TRecord> *LocalAddress(gid::Gid gid) const {
    const auto &map = GetMap<TRecord>();
    auto access = map.access();
    auto found = access.find(gid);
    CHECK(found != access.end())
        << "Failed to find "
        << (std::is_same<TRecord, Vertex>::value ? "vertex" : "edge")
        << " for gid: " << gid;
    return found->second;
  }

  /// Converts an address to local, if possible. Returns the same address if
  /// not.
  template <typename TRecord>
  storage::Address<mvcc::VersionList<TRecord>> LocalizedAddressIfPossible(
      storage::Address<mvcc::VersionList<TRecord>> address) const {
    if (address.is_local()) return address;
    if (address.worker_id() == worker_id_) {
      auto vlist = LocalAddress<TRecord>(address.gid());
      if constexpr (std::is_same<TRecord, Vertex>::value)
        return storage::VertexAddress(vlist);
      else
        return storage::EdgeAddress(vlist);
    }
    return address;
  }

  /// Returns remote address for the given local or remote address.
  template <typename TAddress>
  TAddress GlobalizedAddress(TAddress address) const {
    if (address.is_remote()) return address;
    return {address.local()->gid_, worker_id_};
  }

  /// Gets the local edge address for the given gid. Fails if not present.
  mvcc::VersionList<Edge> *LocalEdgeAddress(gid::Gid gid) const;

  /// Gets names of properties stored on disk
  std::vector<std::string> &PropertiesOnDisk() { return properties_on_disk_; }

 private:
  friend class GraphDbAccessor;
  friend class StorageGc;
  friend class distributed::IndexRpcServer;

  int worker_id_;
  gid::Generator vertex_generator_;
  gid::Generator edge_generator_;

  // main storage for the graph
  ConcurrentMap<gid::Gid, mvcc::VersionList<Vertex> *> vertices_;
  ConcurrentMap<gid::Gid, mvcc::VersionList<Edge> *> edges_;

  // indexes
  KeyIndex<storage::Label, Vertex> labels_index_;
  LabelPropertyIndex label_property_index_;

  std::vector<std::string> properties_on_disk_;

  // Set of transactions ids which are building indexes currently
  SkipList<tx::TransactionId> index_build_tx_in_progress_;

  /// Gets the Vertex/Edge main storage map.
  template <typename TRecord>
  const ConcurrentMap<gid::Gid, mvcc::VersionList<TRecord> *> &GetMap() const;
};

template <>
inline const ConcurrentMap<gid::Gid, mvcc::VersionList<Vertex> *>
    &Storage::GetMap() const {
  return vertices_;
}

template <>
inline const ConcurrentMap<gid::Gid, mvcc::VersionList<Edge> *>
    &Storage::GetMap() const {
  return edges_;
}

}  // namespace database
