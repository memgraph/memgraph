/// @file

#pragma once

#include <map>
#include <vector>

// Forward declaration
namespace database {
class GraphDb;
}  // namespace database

namespace raft {

// Forward declaration
class Coordination;

/// StorageInfo takes care of the Raft cluster storage info retrieval.
class StorageInfo final {
 public:
  StorageInfo() = delete;
  StorageInfo(database::GraphDb *db, Coordination *coordination,
              uint16_t server_id);

  StorageInfo(const StorageInfo &) = delete;
  StorageInfo(StorageInfo &&) = delete;
  StorageInfo operator=(const StorageInfo &) = delete;
  StorageInfo operator=(StorageInfo &&) = delete;

  ~StorageInfo();

  void Start();

  /// Returns storage info for the local storage only.
  std::vector<std::pair<std::string, std::string>> GetLocalStorageInfo() const;

  /// Returns storage info for each peer in the Raft cluster.
  std::map<std::string, std::vector<std::pair<std::string, std::string>>>
  GetStorageInfo() const;

 private:
  database::GraphDb *db_{nullptr};
  Coordination *coordination_{nullptr};
  uint16_t server_id_;
};

}  // namespace raft
