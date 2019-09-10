#pragma once

#include "durability/single_node_ha/state_delta.hpp"

namespace storage {

class StateDeltaBuffer final {
 public:
  /// Inserts a new StateDelta in buffer.
  void Emplace(const database::StateDelta &delta) {
    tx::TransactionId tx_id = delta.transaction_id;
    std::vector<database::StateDelta> *curr_buffer;
    {
      // We only need the lock when we're inserting a new key into the buffer.
      std::lock_guard<std::mutex> lock(buffer_lock_);
      curr_buffer = &buffer_[tx_id];
    }
    curr_buffer->emplace_back(delta);
  }

  /// Retrieves all buffered StateDeltas for a given transaction id.
  /// If there are no such StateDeltas, the return vector is empty.
  std::vector<database::StateDelta> GetDeltas(
      const tx::TransactionId &tx_id) {
    std::vector<database::StateDelta> *curr_buffer;
    {
      std::lock_guard<std::mutex> lock(buffer_lock_);
      auto it = buffer_.find(tx_id);
      if (it == buffer_.end()) return {};
      curr_buffer = &it->second;
    }
    return *curr_buffer;
  }

  /// Deletes all buffered StateDeltas for a given transaction id.
  void Erase(const tx::TransactionId &tx_id) {
    std::lock_guard<std::mutex> lock(buffer_lock_);
    buffer_.erase(tx_id);
  }

 private:
  mutable std::mutex buffer_lock_;
  std::unordered_map<tx::TransactionId, std::vector<database::StateDelta>>
      buffer_;
};

}  // namespace storage
