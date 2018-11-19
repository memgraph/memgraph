/// @file

#pragma once

#include "raft/config.hpp"
#include "raft/coordination.hpp"

#include "storage/common/kvstore/kvstore.hpp"

namespace raft {

enum class Mode { FOLLOWER, CANDIDATE, LEADER };

/**
 * Class which models the behaviour of a single server within the Raft
 * cluster. The class is responsible for storing both volatile and
 * persistent internal state of the corresponding state machine as well
 * as performing operations that comply with the Raft protocol.
 */
class RaftServer {
 public:
  RaftServer() = delete;

  /**
   * The implementation assumes that server IDs are unique integers between
   * ranging from 1 to cluster_size.
   *
   * @param server_id ID of the current server.
   * @param config Configurable Raft parameters (e.g. timeout interval)
   * @param coordination Abstraction for coordination between Raft servers.
   */
  RaftServer(uint16_t server_id, const raft::Config &config,
             raft::Coordination *coordination);

 private:
  /** volatile state on all servers **/

  raft::Mode mode_;      ///< Server's current mode.
  raft::Config config_;  ///< Raft config.

  uint16_t server_id_;     ///< ID of the current server.
  uint64_t commit_index_;  ///< Index of the highest known commited entry.
  uint64_t last_applied_;  ///< Index of the highest applied entry to SM.

  /** volatile state on leaders **/

  std::vector<uint16_t> next_index_;  ///< for each server, index of the next
                                      ///< log entry to send to that server.

  std::vector<uint16_t> match_index_;  ///< for each server, index of the
                                       ///< highest log entry known to be
                                       ///< replicated on server.

  /** persistent state on all servers
   *
   * Persistent data consists of:
   *   - uint64_t current_term -- latest term server has seen.
   *   - uint16_t voted_for    -- candidate_id that received vote in current
   *                              term (null if none).
   *   - vector<LogEntry> log  -- log entries.
   */
  storage::KVStore disk_storage_;

  /**
   * Makes a transition to a new `raft::Mode`.
   *
   * @throws InvalidTransitionException when transitioning between uncompatible
   *                                    `raft::Mode`s.
   */
  void Transition(const raft::Mode &new_mode);
};
}  // namespace raft
