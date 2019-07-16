/// @file

#pragma once

#include <mutex>

#include "durability/single_node_ha/state_delta.hpp"
#include "transactions/type.hpp"

namespace raft {

enum class TxStatus { REPLICATED, WAITING, ABORTED, INVALID };

inline std::string TxStatusToString(const TxStatus &tx_status) {
  switch (tx_status) {
    case TxStatus::REPLICATED:
      return "REPLICATED";
    case TxStatus::WAITING:
      return "WAITING";
    case TxStatus::ABORTED:
      return "ABORTED";
    case TxStatus::INVALID:
      return "INVALID";
  }
}

/// Structure which describes the StateDelta status after the execution of
/// RaftServer's Emplace method.
///
/// It consists of two fields:
///   1) A boolean flag `emplaced` which signals whether the delta has
///      successfully been emplaced in the raft log buffer.
///   2) Two optional unsigned 64-bit integers which denote the term
///      when the corresponding LogEntry was emplaced and its log_index in
///      the Raft log. These values are contained in the optional metadata only
///      if the emplaced StateDelta signifies the COMMIT of a non-read-only
///      transaction.
struct DeltaStatus {
  bool emplaced;
  std::optional<uint64_t> term_id;
  std::optional<uint64_t> log_index;
};

/// Exposes only functionality that other parts of Memgraph can interact with,
/// emplacing a state delta into the appropriate Raft log entry.
class RaftInterface {
 public:
  /// Add StateDelta to the appropriate Raft log entry.
  ///
  /// @returns DeltaStatus object as a result.
  virtual DeltaStatus Emplace(const database::StateDelta &) = 0;

  /// Checks if the transaction with the given transaction id can safely be
  /// committed in local storage.
  virtual bool SafeToCommit(const tx::TransactionId &) = 0;

  /// Returns true if the current servers mode is LEADER. False otherwise.
  virtual bool IsLeader() = 0;

  /// Returns the term ID of the current leader.
  virtual uint64_t TermId() = 0;

  /// Returns the status of the transaction which began its replication in
  /// a given term ID and was emplaced in the raft log at the given index.
  ///
  /// Transaction status can be one of the following:
  ///   1) REPLICATED -- transaction was successfully replicated accross
  ///                    the Raft cluster
  ///   2) WAITING    -- transaction was successfully emplaced in the Raft
  ///                    log and is currently being replicated.
  ///   3) ABORTED    -- transaction was aborted.
  ///   4) INVALID    -- the request for the transaction was invalid, most
  ///                    likely either term_id or log_index were out of range.
  virtual TxStatus TransactionStatus(uint64_t term_id, uint64_t log_index) = 0;

  virtual std::mutex &WithLock() = 0;

 protected:
  ~RaftInterface() {}
};

}  // namespace raft
